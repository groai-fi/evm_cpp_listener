#pragma once

#include <cstddef>
#include <list>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_map>

#include "pipeline/event.hpp"

namespace evml::pipeline {

// Layer-1 dedup (plan 3.7, rev 5): bounded LRU map `event_id -> last emitted type`. An entry is emitted only when
// its type differs from the last one emitted for that event_id, so `log, log` collapses but a log that becomes
// canonical again after a revert (A->B->A) passes (I6). An id this process has not seen (or has evicted) is
// always admitted: Redis (Phase 4 emit.lua, the same rule) is the authority across restarts and hosts.
class LastTypeMap {
public:
    // Throws std::invalid_argument for capacity == 0.
    explicit LastTypeMap(std::size_t capacity) : capacity_(capacity) {
        if (capacity == 0) throw std::invalid_argument("LastTypeMap: capacity must be > 0");
    }

    // true = emit (and `type` is recorded as the last one); false = same type as the last emitted: duplicate.
    bool admit(const std::string& event_id, EventType type) {
        const auto it = index_.find(event_id);
        if (it != index_.end()) {
            order_.splice(order_.begin(), order_, it->second);
            if (it->second->second == type) return false;
            it->second->second = type;
            return true;
        }
        order_.emplace_front(event_id, type);
        index_.emplace(event_id, order_.begin());
        if (order_.size() > capacity_) {
            index_.erase(order_.back().first);
            order_.pop_back();
        }
        return true;
    }

    std::optional<EventType> last(const std::string& event_id) const {
        const auto it = index_.find(event_id);
        if (it == index_.end()) return std::nullopt;
        return it->second->second;
    }
    std::size_t size() const { return order_.size(); }

private:
    using Entry = std::pair<std::string, EventType>;
    std::size_t capacity_;
    std::list<Entry> order_;  // most recent first
    std::unordered_map<std::string, std::list<Entry>::iterator> index_;
};

}  // namespace evml::pipeline
