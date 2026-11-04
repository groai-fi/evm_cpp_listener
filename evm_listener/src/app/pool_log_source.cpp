#include "app/pool_log_source.hpp"

#include <algorithm>
#include <stdexcept>

#include "core/error.hpp"
#include "core/hex.hpp"

namespace net = boost::asio;

namespace evml::app {

struct PoolLogSource::State {
    State(std::shared_ptr<provider::ProviderPool> p, const chain::ReorgDetector* r)
        : pool(std::move(p)), ring(r) {}

    void cache_block(std::uint64_t number, const pipeline::BlockInfo& info) {
        if (block_cache.find(number) == block_cache.end()) {
            while (block_cache.size() >= kMaxBlockCache && !cache_order.empty()) {
                block_cache.erase(cache_order.front());
                cache_order.pop_front();
            }
            cache_order.push_back(number);
        }
        block_cache[number] = info;
    }

    void clear_cache() {
        block_cache.clear();
        cache_order.clear();
    }

    std::shared_ptr<provider::ProviderPool> pool;
    const chain::ReorgDetector* ring;
    rpc::Decoder dec;                                // used only synchronously (never across a co_await)
    std::unordered_set<std::string> avoid;           // providers reported bad since the last get_logs
    std::unordered_set<std::string> logs_providers;  // every provider that answered get_logs for the current page
    std::optional<pipeline::BlockRange> last_logs_range;
    std::vector<core::Address20> seen_split_addresses;
    bool page_sealed{false};
    std::unordered_map<std::uint64_t, pipeline::BlockInfo> block_cache;
    std::deque<std::uint64_t> cache_order;
};

PoolLogSource::PoolLogSource(std::shared_ptr<provider::ProviderPool> pool, const chain::ReorgDetector* ring) {
    if (!pool) throw std::invalid_argument("PoolLogSource: null pool");
    state_ = std::make_shared<State>(std::move(pool), ring);
}

std::optional<std::string> PoolLogSource::current_provider() const {
    if (state_->avoid.empty()) return state_->pool->current_getlogs();
    if (auto p = state_->pool->pick_getlogs_excluding(state_->avoid)) return p;
    return state_->pool->current_getlogs();  // nobody else: ask the same provider again
}

void PoolLogSource::report_bad(std::string_view provider, std::string_view reason) {
    state_->pool->report_bad(provider, reason);
    state_->avoid.emplace(provider);
    state_->page_sealed = true;
    state_->clear_cache();
}

void PoolLogSource::clear_cache() { state_->clear_cache(); }

net::awaitable<pipeline::LogResult> PoolLogSource::get_logs(pipeline::BlockRange range,
                                                            const core::LogFilter& filter_ref) {
    auto st = state_;
    const core::LogFilter filter = filter_ref;
    const bool same_split_page =
        st->last_logs_range && st->last_logs_range->from == range.from && st->last_logs_range->to == range.to &&
        !st->page_sealed && filter.addresses.size() == 1 &&
        std::find(st->seen_split_addresses.begin(), st->seen_split_addresses.end(), filter.addresses.front()) ==
            st->seen_split_addresses.end();
    if (!same_split_page) {
        st->logs_providers.clear();
        st->seen_split_addresses.clear();
        st->page_sealed = false;
        st->last_logs_range = range;
    }
    if (filter.addresses.size() == 1) {
        st->seen_split_addresses.push_back(filter.addresses.front());
    }
    std::unordered_set<std::string> tried = std::move(st->avoid);  // steer this call away from providers reported bad
    st->avoid.clear();
    const bool steered = !tried.empty();
    bool attempted = false;

    pipeline::LogResult last;
    last.kind = rpc::FailKind::Retry;
    last.error = "no usable getLogs provider";
    for (;;) {
        auto p = st->pool->pick_getlogs_excluding(tried);
        if (!p && steered && !attempted) p = st->pool->current_getlogs();  // only the reported provider is usable
        if (!p) co_return last;
        tried.insert(*p);
        attempted = true;

        const auto o = co_await st->pool->call_to(*p, [&](std::uint64_t id) {
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
        auto parsed = st->dec.try_parse_logs(o.result_raw);
        if (!parsed) {
            st->pool->report_bad(o.provider, "undecodable getLogs response: " + parsed.error().what());
            last.kind = rpc::FailKind::Retry;
            last.range_limit.reset();
            last.error = "undecodable getLogs response: " + parsed.error().what();
            continue;
        }
        r.logs = std::move(*parsed);
        r.ok = true;
        st->logs_providers.insert(o.provider);
        co_return r;
    }
}

net::awaitable<std::optional<pipeline::BlockInfo>> PoolLogSource::block(std::uint64_t number) {
    auto st = state_;
    st->page_sealed = true;
    std::optional<core::Hash32> ring_hash;
    if (st->ring) {
        if (const auto ref = st->ring->ref_at(number)) {
            if (ref->timestamp != 0) {
                const pipeline::BlockInfo info{ref->hash, ref->timestamp};
                st->cache_block(number, info);
                co_return info;
            }
            ring_hash = ref->hash;  // hash known, timestamp not: read the block and cross-check the hash
        }
    }

    if (const auto it = st->block_cache.find(number); it != st->block_cache.end()) {
        if (!ring_hash || *ring_hash == it->second.hash) {
            co_return it->second;
        }
        st->block_cache.erase(it);
        st->cache_order.erase(std::remove(st->cache_order.begin(), st->cache_order.end(), number),
                              st->cache_order.end());
    }

    std::unordered_set<std::string> tried = st->logs_providers;  // independence: never any provider that gave the logs
    for (;;) {
        const auto p = st->pool->pick_getlogs_excluding(tried);
        if (!p) co_return std::nullopt;  // no independent provider left: no quorum
        tried.insert(*p);

        const auto o = co_await st->pool->call_to(*p, [&](std::uint64_t id) {
            return rpc::build_get_block_by_number(id, core::to_hex_u64(number), false);
        });
        if (!o.ok) continue;

        const auto head_res = st->dec.try_parse_block(o.result_raw);
        if (!head_res) {
            st->pool->report_bad(o.provider, "undecodable block " + std::to_string(number) + ": " + head_res.error().what());
            continue;
        }
        const auto& head = *head_res;
        if (!head) continue;  // this provider does not have the block (yet): not its fault
        if (head->number != number) {
            st->pool->report_bad(o.provider, "asked for block " + std::to_string(number) + ", got " +
                                                 std::to_string(head->number));
            continue;
        }
        if (ring_hash && !(*ring_hash == head->hash)) {
            st->pool->report_bad(o.provider, "block " + std::to_string(number) + " hash " + head->hash.to_hex() +
                                                 " disagrees with the head ring (" + ring_hash->to_hex() + ")");
            co_return std::nullopt;
        }
        const pipeline::BlockInfo info{head->hash, head->timestamp};
        st->cache_block(number, info);
        co_return info;
    }
}

}  // namespace evml::app
