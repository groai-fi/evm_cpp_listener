#pragma once

#include "redis_client_base.h"
#include <vector>
#include <unordered_map>
#include <optional>

class RedisStreamClient : public RedisClientBase {
public:
    explicit RedisStreamClient(const string& redis_url);
    ~RedisStreamClient() override = default;

    struct StreamEntry {
        string id;
        unordered_map<string, string> fields;
    };

    optional<string> xadd(const string& key, const unordered_map<string, string>& fields, const string& id = "*");
    optional<unordered_map<string, vector<StreamEntry>>> xread(const vector<string>& keys, const vector<string>& ids, optional<int> count = nullopt, optional<int> block_ms = nullopt);
};
