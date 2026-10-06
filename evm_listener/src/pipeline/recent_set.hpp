#pragma once

#include <cstddef>
#include <list>
#include <stdexcept>
#include <string>
#include <unordered_map>

namespace evml::pipeline {

// Bounded set of recently seen keys with LRU eviction: layer-1 dedup (plan 3.7), absorbing safe/final-sweep and
// failover overlap without Redis round-trips. Redis (Phase 4) stays the authority across restarts and hosts.
class RecentSet {
public:
    // Throws std::invalid_argument for capacity == 0.
    explicit RecentSet(std::size_t capacity) : capacity_(capacity) {
        if (capacity == 0) throw std::invalid_argument("RecentSet: capacity must be > 0");
    }

    // true = the key is new (inserted); false = already present (refreshed as most recent).
    bool insert(const std::string& key) {
        const auto it = index_.find(key);
        if (it != index_.end()) {
            order_.splice(order_.begin(), order_, it->second);
            return false;
        }
        order_.push_front(key);
        index_.emplace(key, order_.begin());
        if (order_.size() > capacity_) {
            index_.erase(order_.back());
            order_.pop_back();
        }
        return true;
    }

    bool contains(const std::string& key) const { return index_.count(key) != 0; }
    std::size_t size() const { return order_.size(); }

private:
    std::size_t capacity_;
    std::list<std::string> order_;  // most recent first
    std::unordered_map<std::string, std::list<std::string>::iterator> index_;
};

}  // namespace evml::pipeline
