#pragma once

#include "redis_client_base.h"
#include <nlohmann/json.hpp>
#include <optional>
#include <chrono>
#include <unordered_map>

using json = nlohmann::json;

class RedisKVClient : public RedisClientBase {
public:
    explicit RedisKVClient(const string& redis_url, chrono::seconds cache_ttl = chrono::seconds(300));
    ~RedisKVClient() override = default;

    // Basic Redis operations
    optional<string> get(const string& key, bool use_cache = false);
    bool set(const string& key, const string& value, optional<int> expiry_seconds = nullopt);
    bool del(const string& key);
    bool exists(const string& key);

    // Parses the string value of key as a double. Returns nullopt on missing
    // key, parse failure, or NaN (treats "nan" strings as missing).
    optional<double> get_double(const string& key, bool use_cache = false);

    // JSON operations
    optional<json> get_json(const string& key, bool use_cache = false);
    bool set_json(const string& key, const json& value, optional<int> expiry_seconds = nullopt);

    // Pub/Sub operations
    long long publish(const string& channel, const string& message);
    
    // Cache management
    void clear_cache();
    void set_cache_ttl(chrono::seconds ttl);
    size_t cache_size() const;

private:
    struct CacheEntry {
        string data;
        chrono::steady_clock::time_point expiry_time;
        bool is_expired() const { return chrono::steady_clock::now() > expiry_time; }
    };

    chrono::seconds default_cache_ttl_;
    unordered_map<string, CacheEntry> cache_;
    mutable mutex cache_mutex_;
};
