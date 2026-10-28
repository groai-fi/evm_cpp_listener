#include "app/pool_log_source.hpp"

#include <stdexcept>

#include "core/error.hpp"
#include "core/hex.hpp"

namespace net = boost::asio;

namespace evml::app {

PoolLogSource::PoolLogSource(std::shared_ptr<provider::ProviderPool> pool, const chain::ReorgDetector* ring)
    : pool_(std::move(pool)), ring_(ring) {
    if (!pool_) throw std::invalid_argument("PoolLogSource: null pool");
}

std::optional<std::string> PoolLogSource::current_provider() const {
    if (avoid_.empty()) return pool_->current_getlogs();
    if (auto p = pool_->pick_getlogs_excluding(avoid_)) return p;
    return pool_->current_getlogs();  // nobody else: ask the same provider again
}

void PoolLogSource::report_bad(const std::string& provider, const std::string& reason) {
    pool_->report_bad(provider, reason);
    avoid_.insert(provider);
}

void PoolLogSource::cache_block(std::uint64_t number, const pipeline::BlockInfo& info) {
    if (block_cache_.find(number) == block_cache_.end()) {
        while (block_cache_.size() >= kMaxBlockCache && !cache_order_.empty()) {
            block_cache_.erase(cache_order_.front());
            cache_order_.pop_front();
        }
        cache_order_.push_back(number);
    }
    block_cache_[number] = info;
}

void PoolLogSource::clear_cache() {
    block_cache_.clear();
    cache_order_.clear();
}

net::awaitable<pipeline::LogResult> PoolLogSource::get_logs(pipeline::BlockRange range,
                                                            const core::LogFilter& filter) {
    std::unordered_set<std::string> tried = std::move(avoid_);  // steer this call away from providers reported bad
    avoid_.clear();
    const bool steered = !tried.empty();
    bool attempted = false;

    pipeline::LogResult last;
    last.kind = rpc::FailKind::Retry;
    last.error = "no usable getLogs provider";
    for (;;) {
        auto p = pool_->pick_getlogs_excluding(tried);
        if (!p && steered && !attempted) p = pool_->current_getlogs();  // only the reported provider is usable
        if (!p) co_return last;
        tried.insert(*p);
        attempted = true;

        const auto o = co_await pool_->call_to(*p, [&](std::uint64_t id) {
            return rpc::build_get_logs(id, range.from, range.to, filter);
        });
        last.provider = o.provider;
        if (!o.ok) {
            last.kind = o.kind;
            last.range_limit = o.range_limit;
            last.error = o.error;
            if (o.kind == rpc::FailKind::RangeTooLarge) co_return last;  // the caller's range, not the provider's fault
            continue;
        }

        pipeline::LogResult r;
        r.provider = o.provider;
        try {
            r.logs = dec_.parse_logs(o.result_raw);
        } catch (const core::DecodeError& e) {
            pool_->report_bad(o.provider, std::string("undecodable getLogs response: ") + e.what());
            last.kind = rpc::FailKind::Retry;
            last.range_limit.reset();
            last.error = std::string("undecodable getLogs response: ") + e.what();
            continue;
        }
        r.ok = true;
        logs_provider_ = o.provider;
        co_return r;
    }
}

net::awaitable<std::optional<pipeline::BlockInfo>> PoolLogSource::block(std::uint64_t number) {
    std::optional<core::Hash32> ring_hash;
    if (ring_) {
        if (const auto ref = ring_->ref_at(number)) {
            if (ref->timestamp != 0) {
                const pipeline::BlockInfo info{ref->hash, ref->timestamp};
                cache_block(number, info);
                co_return info;
            }
            ring_hash = ref->hash;  // hash known, timestamp not: read the block and cross-check the hash
        }
    }

    if (const auto it = block_cache_.find(number); it != block_cache_.end()) {
        if (!ring_hash || *ring_hash == it->second.hash) {
            co_return it->second;
        }
        block_cache_.erase(it);
    }

    std::unordered_set<std::string> tried;
    if (!logs_provider_.empty()) tried.insert(logs_provider_);  // independence: never the provider that gave the logs
    for (;;) {
        const auto p = pool_->pick_getlogs_excluding(tried);
        if (!p) co_return std::nullopt;  // no independent provider left: no quorum
        tried.insert(*p);

        const auto o = co_await pool_->call_to(*p, [&](std::uint64_t id) {
            return rpc::build_get_block_by_number(id, core::to_hex_u64(number), false);
        });
        if (!o.ok) continue;

        std::optional<rpc::Head> head;
        try {
            head = dec_.parse_block(o.result_raw);
        } catch (const core::DecodeError& e) {
            pool_->report_bad(o.provider, std::string("undecodable block ") + std::to_string(number) + ": " + e.what());
            continue;
        }
        if (!head) continue;  // this provider does not have the block (yet): not its fault
        if (head->number != number) {
            pool_->report_bad(o.provider, "asked for block " + std::to_string(number) + ", got " +
                                              std::to_string(head->number));
            continue;
        }
        if (ring_hash && !(*ring_hash == head->hash)) {
            pool_->report_bad(o.provider, "block " + std::to_string(number) + " hash " + head->hash.to_hex() +
                                              " disagrees with the head ring (" + ring_hash->to_hex() + ")");
            co_return std::nullopt;
        }
        const pipeline::BlockInfo info{head->hash, head->timestamp};
        cache_block(number, info);
        co_return info;
    }
}

}  // namespace evml::app
