#include "redis_kv_client.h"
#include <iostream>

RedisKVClient::RedisKVClient(const string& redis_url, chrono::seconds cache_ttl)
    : RedisClientBase(redis_url), default_cache_ttl_(cache_ttl) {}

optional<string> RedisKVClient::get(const string& key, bool use_cache) {
    if (key.empty()) {
        log_message("ERROR", "get", "Empty key provided");
        return nullopt;
    }

    if (use_cache) {
        lock_guard<mutex> cache_lock(cache_mutex_);
        auto it = cache_.find(key);
        if (it != cache_.end() && !it->second.is_expired()) {
            return it->second.data;
        }
    }

    auto command = [&](redisContext* c) {
        return (redisReply*)redisCommand(c, "GET %s", key.c_str());
    };
    
    redisReply* reply = execute_command(command);

    if (!reply) {
        log_message("ERROR", "get", "No reply received for key: " + key);
        return nullopt;
    }

    if (reply->type == REDIS_REPLY_ERROR) {
        log_message("ERROR", "get", "Redis error for key " + key + ": " + reply->str);
        freeReplyObject(reply);
        return nullopt;
    }

    if (reply->type == REDIS_REPLY_NIL) {
        freeReplyObject(reply);
        return nullopt;
    }

    if (reply->type != REDIS_REPLY_STRING) {
        log_message("ERROR", "get", "Unexpected reply type for key: " + key);
        freeReplyObject(reply);
        return nullopt;
    }

    string result(reply->str, reply->len);
    freeReplyObject(reply);

    if (use_cache) {
        lock_guard<mutex> cache_lock(cache_mutex_);
        cache_[key] = {result, chrono::steady_clock::now() + default_cache_ttl_};
    }

    return result;
}

bool RedisKVClient::set(const string& key, const string& value, optional<int> expiry_seconds) {
    if (key.empty()) {
        log_message("ERROR", "set", "Empty key provided");
        return false;
    }

    auto command = [&](redisContext* c) -> redisReply* {
        if (expiry_seconds.has_value()) {
            return (redisReply*)redisCommand(c, "SETEX %s %d %b", key.c_str(), expiry_seconds.value(), value.c_str(), value.length());
        } else {
            return (redisReply*)redisCommand(c, "SET %s %b", key.c_str(), value.c_str(), value.length());
        }
    };

    redisReply* reply = execute_command(command);

    if (!reply) {
        log_message("ERROR", "set", "No reply received for key: " + key);
        return false;
    }

    bool success = false;
    if (reply->type == REDIS_REPLY_STATUS && strcmp(reply->str, "OK") == 0) {
        success = true;
        lock_guard<mutex> cache_lock(cache_mutex_);
        cache_[key] = {value, chrono::steady_clock::now() + default_cache_ttl_};
    } else {
        log_message("ERROR", "set", "SET failed for key " + key + (reply->type == REDIS_REPLY_ERROR ? ": " + string(reply->str) : ""));
    }

    freeReplyObject(reply);
    return success;
}

bool RedisKVClient::del(const string& key) {
    if (key.empty()) {
        log_message("ERROR", "del", "Empty key provided");
        return false;
    }

    auto command = [&](redisContext* c) {
        return (redisReply*)redisCommand(c, "DEL %s", key.c_str());
    };

    redisReply* reply = execute_command(command);

    if (!reply) {
        log_message("ERROR", "del", "No reply received for key: " + key);
        return false;
    }

    bool success = false;
    if (reply->type == REDIS_REPLY_INTEGER) {
        success = reply->integer > 0;
        if (success) {
            lock_guard<mutex> cache_lock(cache_mutex_);
            cache_.erase(key);
        }
    } else {
        log_message("ERROR", "del", "DEL failed for key " + key + (reply->type == REDIS_REPLY_ERROR ? ": " + string(reply->str) : ""));
    }

    freeReplyObject(reply);
    return success;
}

bool RedisKVClient::exists(const string& key) {
    if (key.empty()) {
        log_message("ERROR", "exists", "Empty key provided");
        return false;
    }

    auto command = [&](redisContext* c) {
        return (redisReply*)redisCommand(c, "EXISTS %s", key.c_str());
    };
    
    redisReply* reply = execute_command(command);

    if (!reply) {
        log_message("ERROR", "exists", "No reply received for key: " + key);
        return false;
    }

    bool exists_flag = false;
    if (reply->type == REDIS_REPLY_INTEGER) {
        exists_flag = reply->integer > 0;
    } else {
        log_message("ERROR", "exists", "EXISTS failed for key " + key + (reply->type == REDIS_REPLY_ERROR ? ": " + string(reply->str) : ""));
    }

    freeReplyObject(reply);
    return exists_flag;
}

optional<double> RedisKVClient::get_double(const string& key, bool use_cache) {
    auto str_result = get(key, use_cache);
    if (!str_result.has_value()) {
        return nullopt;
    }
    try {
        double parsed = std::stod(str_result.value());
        if (std::isnan(parsed)) return nullopt;  // treat "nan" strings as missing
        return parsed;
    } catch (const std::exception& e) {
        log_message("ERROR", "get_double",
                    "Failed to parse double from key '" + key + "': " + e.what() +
                    ". Raw data: '" + str_result.value() + "'");
        return nullopt;
    }
}

optional<json> RedisKVClient::get_json(const string& key, bool use_cache) {
    auto str_result = get(key, use_cache);
    if (!str_result.has_value()) {
        return nullopt;
    }

    try {
        return json::parse(str_result.value());
    } catch (const json::exception& e) {
        log_message("ERROR", "get_json", "Failed to parse JSON from key '" + key + "': " + e.what() + ". Raw data: '" + str_result.value() + "'");
        return nullopt;
    }
}

bool RedisKVClient::set_json(const string& key, const json& value, optional<int> expiry_seconds) {
    try {
        string json_str = value.dump();
        return set(key, json_str, expiry_seconds);
    } catch (const json::exception& e) {
        log_message("ERROR", "set_json", "Failed to serialize JSON for key '" + key + "': " + e.what());
        return false;
    }
}

long long RedisKVClient::publish(const string& channel, const string& message) {
    if (channel.empty()) {
        log_message("ERROR", "publish", "Empty channel provided");
        return -1;
    }
    
    auto command = [&](redisContext* c) {
        return (redisReply*)redisCommand(c, "PUBLISH %s %b", channel.c_str(), message.c_str(), message.length());
    };
    
    redisReply* reply = execute_command(command);

    if (!reply) {
        log_message("ERROR", "publish", "No reply received for channel: " + channel);
        return -1;
    }

    long long receivers = -1;
    if (reply->type == REDIS_REPLY_INTEGER) {
        receivers = reply->integer;
    } else {
        log_message("ERROR", "publish", "PUBLISH failed for channel " + channel + (reply->type == REDIS_REPLY_ERROR ? ": " + string(reply->str) : ""));
    }

    freeReplyObject(reply);
    return receivers;
}

void RedisKVClient::clear_cache() {
    lock_guard<mutex> lock(cache_mutex_);
    cache_.clear();
}

void RedisKVClient::set_cache_ttl(chrono::seconds ttl) {
    default_cache_ttl_ = ttl;
}

size_t RedisKVClient::cache_size() const {
    lock_guard<mutex> lock(cache_mutex_);
    return cache_.size();
}
