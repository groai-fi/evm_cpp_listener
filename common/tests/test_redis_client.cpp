#include "redis_kv_client.h"
#include "redis_stream_client.h"
#include <cassert>
#include <iostream>
#include <nlohmann/json.hpp>
#include <thread>

using namespace std;
using json = nlohmann::json;

void test_kv_operations() {
    cout << "--- Testing KV Operations ---" << endl;
    RedisKVClient client("redis://localhost:6379");
    assert(client.connect());

    // Test SET/GET
    assert(client.set("test_key", "test_value"));
    auto val = client.get("test_key");
    assert(val.has_value() && val.value() == "test_value");
    cout << "✅ SET/GET passed" << endl;

    // Test DEL
    assert(client.del("test_key"));
    assert(!client.get("test_key").has_value());
    cout << "✅ DEL passed" << endl;

    // Test JSON
    json j = {{"name", "test"}, {"value", 123}};
    assert(client.set_json("test_json", j));
    auto j_val = client.get_json("test_json");
    assert(j_val.has_value() && j_val.value() == j);
    cout << "✅ JSON passed" << endl;
}

void test_stream_operations() {
    cout << "--- Testing Stream Operations ---" << endl;
    RedisStreamClient client("redis://localhost:6379");
    assert(client.connect());

    // Test XADD
    unordered_map<string, string> fields = {{"field1", "value1"}, {"field2", "value2"}};
    auto id = client.xadd("test_stream", fields);
    assert(id.has_value());
    cout << "✅ XADD passed" << endl;

    // Test XREAD
    auto result_opt = client.xread({"test_stream"}, {"0-0"});
    assert(result_opt.has_value());
    auto& result = *result_opt;
    assert(!result.empty() && result.count("test_stream"));
    assert(!result["test_stream"].empty());
    assert(result["test_stream"][0].fields["field1"] == "value1");
    cout << "✅ XREAD passed" << endl;
}

int main() {
    try {
        test_kv_operations();
        test_stream_operations();
    } catch (const exception& e) {
        cerr << "Error: " << e.what() << endl;
        return 1;
    }
    return 0;
}
