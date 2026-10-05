#include "redis_stream_client.h"
#include <iostream>

RedisStreamClient::RedisStreamClient(const string& redis_url) : RedisClientBase(redis_url) {}

optional<string> RedisStreamClient::xadd(const string& key, const unordered_map<string, string>& fields, const string& id) {
    if (key.empty() || fields.empty()) {
        log_message("ERROR", "xadd", "Empty key or fields provided");
        return nullopt;
    }

    vector<const char*> argv;
    vector<size_t> argvlen;
    string cmd = "XADD";
    argv.push_back(cmd.c_str());
    argvlen.push_back(cmd.length());
    argv.push_back(key.c_str());
    argvlen.push_back(key.length());
    argv.push_back(id.c_str());
    argvlen.push_back(id.length());

    for (const auto& pair : fields) {
        argv.push_back(pair.first.c_str());
        argvlen.push_back(pair.first.length());
        argv.push_back(pair.second.c_str());
        argvlen.push_back(pair.second.length());
    }
    
    auto command = [&](redisContext* c) {
        return (redisReply*)redisCommandArgv(c, argv.size(), argv.data(), argvlen.data());
    };

    redisReply* reply = execute_command(command);

    if (!reply) {
        log_message("ERROR", "xadd", "No reply received");
        return nullopt;
    }

    if (reply->type == REDIS_REPLY_ERROR) {
        log_message("ERROR", "xadd", "Redis error: " + string(reply->str));
        freeReplyObject(reply);
        return nullopt;
    }

    string new_id;
    if (reply->type == REDIS_REPLY_STRING || reply->type == REDIS_REPLY_STATUS) {
        new_id = string(reply->str, reply->len);
    } else {
        log_message("ERROR", "xadd", "Unexpected reply type");
    }

    freeReplyObject(reply);
    return new_id.empty() ? nullopt : optional<string>(new_id);
}

optional<unordered_map<string, vector<RedisStreamClient::StreamEntry>>> RedisStreamClient::xread(
    const vector<string>& keys, const vector<string>& ids, optional<int> count, optional<int> block_ms) {

    if (keys.size() != ids.size() || keys.empty()) {
        log_message("ERROR", "xread", "Invalid keys/ids mismatch or empty");
        return nullopt;
    }

    vector<string> args;
    args.push_back("XREAD");

    if (count.has_value()) {
        args.push_back("COUNT");
        args.push_back(to_string(count.value()));
    }

    if (block_ms.has_value()) {
        args.push_back("BLOCK");
        args.push_back(to_string(block_ms.value()));
    }

    args.push_back("STREAMS");
    for (const auto& k : keys) args.push_back(k);
    for (const auto& i : ids) args.push_back(i);

    vector<const char*> argv;
    vector<size_t> argvlen;
    argv.reserve(args.size());
    argvlen.reserve(args.size());

    for (const auto& arg : args) {
        argv.push_back(arg.c_str());
        argvlen.push_back(arg.length());
    }

    auto command_fn = [&](redisContext* c) {
        return (redisReply*)redisCommandArgv(c, argv.size(), argv.data(), argvlen.data());
    };

    redisReply* reply = execute_command(command_fn);

    if (!reply) {
        log_message("ERROR", "xread", "No reply received");
        return nullopt;
    }

    if (reply->type == REDIS_REPLY_ERROR) {
        log_message("ERROR", "xread", "Redis error: " + string(reply->str));
        freeReplyObject(reply);
        return nullopt;
    }

    if (reply->type == REDIS_REPLY_NIL) {
        freeReplyObject(reply);
        return unordered_map<string, vector<StreamEntry>>{}; // Timeout
    }

    if (reply->type != REDIS_REPLY_ARRAY) {
        log_message("ERROR", "xread", "Unexpected reply type");
        freeReplyObject(reply);
        return nullopt;
    }

    unordered_map<string, vector<StreamEntry>> result;
    for (size_t i = 0; i < reply->elements; i++) {
        redisReply* stream_reply = reply->element[i];
        if (stream_reply->type != REDIS_REPLY_ARRAY || stream_reply->elements != 2) continue;

        string stream_key(stream_reply->element[0]->str, stream_reply->element[0]->len);
        redisReply* entries_reply = stream_reply->element[1];

        if (entries_reply->type != REDIS_REPLY_ARRAY) continue;

        vector<StreamEntry> entries;
        for (size_t j = 0; j < entries_reply->elements; j++) {
            redisReply* entry_reply = entries_reply->element[j];
            if (entry_reply->type != REDIS_REPLY_ARRAY || entry_reply->elements != 2) continue;

            StreamEntry entry;
            entry.id = string(entry_reply->element[0]->str, entry_reply->element[0]->len);

            redisReply* fields_reply = entry_reply->element[1];
            if (fields_reply->type == REDIS_REPLY_ARRAY) {
                for (size_t k = 0; k < fields_reply->elements; k += 2) {
                    string f_key(fields_reply->element[k]->str, fields_reply->element[k]->len);
                    string f_val(fields_reply->element[k + 1]->str, fields_reply->element[k + 1]->len);
                    entry.fields[f_key] = f_val;
                }
            }
            entries.push_back(entry);
        }
        result[stream_key] = entries;
    }

    freeReplyObject(reply);
    return result;
}
