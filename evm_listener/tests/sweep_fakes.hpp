#pragma once

#include <algorithm>
#include <cstdint>
#include <deque>
#include <future>
#include <map>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_future.hpp>

#include "base_config.hpp"
#include "pipeline/log_source.hpp"
#include "pipeline/sink.hpp"

namespace evml::test {

namespace net = boost::asio;

// Runs one awaitable to completion on a private io_context and returns its result.
template <class T>
T run(net::awaitable<T> aw) {
    net::io_context ioc;
    auto fut = net::co_spawn(ioc, std::move(aw), net::use_future);
    ioc.run();
    return fut.get();
}

inline std::string field(const pipeline::Emission& m, const std::string& key) {
    for (const auto& [k, v] : m.fields)
        if (k == key) return v;
    throw std::logic_error("emission has no field " + key);
}

// In-memory Sink: records everything, models `released` (ZADD on emit with a score, ZREMRANGEBYSCORE on final_trim).
class MemorySink : public pipeline::Sink {
public:
    bool saturated_flag{false};
    bool unreadable{false};  // reads return nullopt, like a degraded or replaying sink (rev 6, A/C)
    std::vector<pipeline::Emission> emitted;
    std::vector<std::pair<pipeline::CheckpointKind, std::uint64_t>> checkpoints;
    std::vector<std::uint64_t> trims;
    pipeline::Checkpoint stored;
    std::size_t contract_writes{0};
    std::size_t init_calls{0};  // init_checkpoint calls that wrote (a checkpoint did not exist yet)
    std::map<std::string, pipeline::ReleasedEntry> released;

    bool saturated() const override { return saturated_flag; }

    void emit(pipeline::Emission e) override {
        if (e.released_block) released[e.event_id] = pipeline::to_released_entry(e);
        if (e.type == pipeline::EventType::Revert) released.erase(e.event_id);  // emit.lua: reverted = no longer released
        emitted.push_back(std::move(e));
    }
    void checkpoint(pipeline::CheckpointKind kind, std::uint64_t block) override {
        checkpoints.emplace_back(kind, block);
        auto& slot = kind == pipeline::CheckpointKind::Safe ? stored.safe : stored.finalized;
        if (!slot || *slot < block) slot = block;  // set-if-greater, like ckpt.lua
    }
    void init_checkpoint(std::uint64_t c0, std::vector<std::string> manifest) override {
        if (stored.safe || stored.finalized || stored.contracts) return;  // ckpt.lua init: only if nothing exists
        ++init_calls;
        stored.safe = c0;
        stored.finalized = c0;
        stored.contracts = std::move(manifest);
    }
    void set_contracts(std::vector<std::string> manifest) override {
        ++contract_writes;
        stored.contracts = std::move(manifest);
    }
    void final_trim(std::uint64_t block) override {
        trims.push_back(block);
        for (auto it = released.begin(); it != released.end();)
            it = it->second.block_number <= block ? released.erase(it) : std::next(it);
    }
    net::awaitable<std::optional<pipeline::Checkpoint>> load_checkpoint() override {
        if (unreadable) co_return std::nullopt;
        co_return stored;
    }
    net::awaitable<std::optional<std::vector<pipeline::ReleasedEntry>>> released_range(std::uint64_t from,
                                                                                       std::uint64_t to) override {
        if (unreadable) co_return std::nullopt;
        std::vector<pipeline::ReleasedEntry> out;
        for (const auto& [id, e] : released)
            if (e.block_number >= from && e.block_number <= to) out.push_back(e);
        co_return out;
    }

    std::size_t count(pipeline::EventType t) const {
        return static_cast<std::size_t>(std::count_if(emitted.begin(), emitted.end(),
                                                      [t](const pipeline::Emission& m) { return m.type == t; }));
    }
};

// Scripted LogSource over a canonical chain: `logs` and `blocks` are the truth; `limit` simulates a provider's
// range cap; one-shot overrides inject failures.
class FakeSource : public pipeline::LogSource {
public:
    std::string provider{"infura"};
    std::vector<rpc::Log> logs;
    std::map<std::uint64_t, pipeline::BlockInfo> blocks;
    std::optional<std::uint64_t> limit;     // blocks per request above which RangeTooLarge is returned
    bool reject_multi_address{false};       // RangeTooLarge for any request naming more than one address
    bool reject_all{false};                 // RangeTooLarge always (even for a single address)
    std::deque<pipeline::LogResult> overrides;  // consumed first, one per get_logs call
    std::set<std::uint64_t> unreadable_blocks;
    bool no_provider{false};

    struct Call {
        pipeline::BlockRange range;
        std::size_t addresses;
    };
    std::vector<Call> calls;
    std::vector<std::pair<std::string, std::string>> bad;  // report_bad(provider, reason) calls

    void report_bad(const std::string& p, const std::string& reason) override { bad.emplace_back(p, reason); }

    std::optional<std::string> current_provider() const override {
        return no_provider ? std::nullopt : std::optional<std::string>(provider);
    }

    net::awaitable<pipeline::LogResult> get_logs(pipeline::BlockRange r, const core::LogFilter& f) override {
        calls.push_back({r, f.addresses.size()});
        if (!overrides.empty()) {
            auto o = std::move(overrides.front());
            overrides.pop_front();
            co_return o;
        }
        pipeline::LogResult res;
        res.provider = provider;
        const bool too_wide = limit && (r.to - r.from + 1) > *limit;
        if (reject_all || too_wide || (reject_multi_address && f.addresses.size() > 1)) {
            res.kind = rpc::FailKind::RangeTooLarge;
            if (too_wide) res.range_limit = static_cast<std::uint32_t>(*limit);
            res.error = "range too large";
            co_return res;
        }
        res.ok = true;
        for (const auto& l : logs) {
            if (l.block_number < r.from || l.block_number > r.to) continue;
            if (std::find(f.addresses.begin(), f.addresses.end(), l.address) == f.addresses.end()) continue;
            if (std::find(f.topic0s.begin(), f.topic0s.end(), l.topics.at(0)) == f.topic0s.end()) continue;
            res.logs.push_back(l);
        }
        co_return res;
    }

    net::awaitable<std::optional<pipeline::BlockInfo>> block(std::uint64_t n) override {
        if (unreadable_blocks.count(n) != 0) co_return std::nullopt;
        const auto it = blocks.find(n);
        if (it == blocks.end()) co_return std::nullopt;
        co_return it->second;
    }
};

// A log of config contract `contract_index` (its first configured event) in block `block` under block hash byte `h`.
inline rpc::Log mklog(const core::Config& cfg, std::size_t contract_index, std::uint64_t block, std::uint8_t h,
                      std::uint64_t log_index) {
    const auto& c = cfg.contracts.at(contract_index);
    rpc::Log l;
    l.address = c.address;
    l.topics = {c.topics.at(0)};
    l.data = "0x";
    l.block_number = block;
    l.block_hash = hash_of(h);
    l.tx_hash = hash_of(0xee);
    l.tx_index = 0;
    l.log_index = log_index;
    return l;
}

inline pipeline::BlockInfo binfo(std::uint8_t h, std::uint64_t ts = 1700000000) {
    return pipeline::BlockInfo{hash_of(h), ts};
}

}  // namespace evml::test
