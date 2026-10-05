// Toolchain smoke test: proves every Conan dependency links and behaves as expected.
#include <catch2/catch_test_macros.hpp>

#include <ethash/keccak.hpp>
#include <openssl/opensslv.h>
#include <prometheus/counter.h>
#include <prometheus/registry.h>
#include <simdjson.h>
#include <spdlog/spdlog.h>
#include <sw/redis++/redis++.h>
#include <toml++/toml.hpp>

#include <boost/json.hpp>

#include <string>

#include "core/version.hpp"

TEST_CASE("keccak256 matches the canonical ERC-20 Transfer topic0", "[unit][toolchain]") {
    const std::string sig = "Transfer(address,address,uint256)";
    const auto h = ethash::keccak256(reinterpret_cast<const uint8_t*>(sig.data()), sig.size());
    static constexpr char kHex[] = "0123456789abcdef";
    std::string hex;
    for (const uint8_t b : h.bytes) {
        hex.push_back(kHex[b >> 4]);
        hex.push_back(kHex[b & 0x0f]);
    }
    CHECK(hex == "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef");
}

TEST_CASE("simdjson parses a JSON-RPC response", "[unit][toolchain]") {
    simdjson::ondemand::parser parser;
    simdjson::padded_string json(std::string_view(R"({"jsonrpc":"2.0","id":1,"result":"0x2a"})"));
    auto doc = parser.iterate(json);
    std::string_view result;
    REQUIRE(doc["result"].get(result) == simdjson::SUCCESS);
    CHECK(result == "0x2a");
}

TEST_CASE("toml++ parses config", "[unit][toolchain]") {
    auto tbl = toml::parse(R"(shard = "base_eurc_usdc")");
    CHECK(tbl["shard"].value_or(std::string_view{}) == "base_eurc_usdc");
}

TEST_CASE("boost.json serializes", "[unit][toolchain]") {
    boost::json::object o;
    o["a"] = 1;
    CHECK(boost::json::serialize(o) == R"({"a":1})");
}

TEST_CASE("spdlog formats", "[unit][toolchain]") {
    CHECK(fmt::format("{}-{}", 1, "x") == "1-x");
}

TEST_CASE("OpenSSL is the pinned 3.5 LTS line", "[unit][toolchain]") {
    CHECK(OPENSSL_VERSION_MAJOR == 3);
    CHECK(OPENSSL_VERSION_MINOR == 5);
}

TEST_CASE("redis++ and prometheus-cpp link", "[unit][toolchain]") {
    // Compile/link-level only: no server connection in a unit test.
    sw::redis::ConnectionOptions opts;
    opts.host = "localhost";
    CHECK(opts.port == 6379);

    prometheus::Registry registry;
    auto& family = prometheus::BuildCounter().Name("evml_smoke_total").Help("smoke").Register(registry);
    auto& c = family.Add({});
    c.Increment();
    CHECK(c.Value() == 1.0);
}

TEST_CASE("version string is populated", "[unit][toolchain]") {
    CHECK(std::string(evml::core::kSemver) == "0.1.0");
    CHECK(std::string(evml::core::version_string()).find("evm_listener 0.1.0") == 0);
}
