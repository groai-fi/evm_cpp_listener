#include <catch2/catch_test_macros.hpp>

#include <fstream>
#include <sstream>

#include "rpc/classify.hpp"
#include "rpc/codec.hpp"

using namespace evml::rpc;

namespace {
std::string read_fixture(const char* name) {
    std::ifstream f(std::string(EVML_FIXTURE_DIR) + "/" + name);
    REQUIRE(f.good());
    std::stringstream ss;
    ss << f.rdbuf();
    return ss.str();
}

// Classify the real captured error bodies end to end (decoder -> ErrorInfo -> classify).
Classified classify_fixture(const char* name, int http_status) {
    Decoder dec;
    const auto err = dec.try_parse_error_body(read_fixture(name));
    REQUIRE(err.has_value());
    ErrorInfo info;
    info.http_status = http_status;
    info.rpc_code = err->code;
    info.message = err->message;
    return classify(info);
}
}  // namespace

TEST_CASE("classify: observed provider fixtures") {
    SECTION("Alchemy monthly quota (HTTP 429) is QuotaExhausted, not RateLimited") {
        CHECK(classify_fixture("err_alchemy_429.json", 429).kind == FailKind::QuotaExhausted);
    }
    SECTION("Infura -32603 service temporarily unavailable is Retry") {
        CHECK(classify_fixture("err_infura_32603.json", 200).kind == FailKind::Retry);
    }
    SECTION("public Base -32614 -> RangeTooLarge with N=500") {
        const auto c = classify_fixture("err_public_32614.json", 200);
        CHECK(c.kind == FailKind::RangeTooLarge);
        CHECK(c.range_limit == 500u);
    }
    SECTION("QuickNode -32615 -> RangeTooLarge with N=5") {
        const auto c = classify_fixture("err_quicknode_32615.json", 413);
        CHECK(c.kind == FailKind::RangeTooLarge);
        CHECK(c.range_limit == 5u);
    }
    SECTION("JSON-RPC parse error is Retry (surfaced, not dropped)") {
        CHECK(classify_fixture("err_null_id.json", 200).kind == FailKind::Retry);
    }
}

TEST_CASE("classify: HTTP status and message rules") {
    const auto k = [](int http, std::optional<std::int64_t> code, const char* msg) {
        ErrorInfo e;
        e.http_status = http;
        e.rpc_code = code;
        e.message = msg;
        return classify(e).kind;
    };
    CHECK(k(429, std::nullopt, "") == FailKind::RateLimited);
    CHECK(k(200, -32005, "limit exceeded") == FailKind::RateLimited);
    CHECK(k(200, std::nullopt, "Too Many Requests") == FailKind::RateLimited);
    CHECK(k(401, std::nullopt, "") == FailKind::AuthFatal);
    CHECK(k(403, std::nullopt, "error code: 1010") == FailKind::AuthFatal);
    CHECK(k(200, std::nullopt, "invalid project id") == FailKind::AuthFatal);
    CHECK(k(200, std::nullopt, "Invalid API Key") == FailKind::AuthFatal);
    CHECK(k(413, std::nullopt, "") == FailKind::RangeTooLarge);
    CHECK(k(200, -32602, "query returned more than 10000 results") == FailKind::RangeTooLarge);
    CHECK(k(200, std::nullopt, "Log response size exceeded") == FailKind::RangeTooLarge);
    CHECK(k(500, std::nullopt, "internal error") == FailKind::Retry);
    CHECK(k(0, std::nullopt, "") == FailKind::Retry);
}

TEST_CASE("classify: range limit parsing") {
    ErrorInfo e;
    e.message = "eth_getLogs is limited to a 10000 range";
    CHECK(classify(e).range_limit == 10000u);
    e.message = "eth_getLogs is limited to a range";  // phrase without digits
    const auto c = classify(e);
    CHECK(c.kind == FailKind::RangeTooLarge);
    CHECK_FALSE(c.range_limit.has_value());
}

TEST_CASE("classify: rate limit carries Retry-After") {
    ErrorInfo e;
    e.http_status = 429;
    e.retry_after = std::chrono::seconds(3);
    const auto c = classify(e);
    CHECK(c.kind == FailKind::RateLimited);
    CHECK(c.retry_after == std::chrono::seconds(3));
}

TEST_CASE("parse_retry_after") {
    CHECK(parse_retry_after("3") == std::chrono::seconds(3));
    CHECK(parse_retry_after("  120 ") == std::chrono::seconds(120));
    CHECK_FALSE(parse_retry_after("").has_value());
    CHECK_FALSE(parse_retry_after("Wed, 21 Oct 2026 07:28:00 GMT").has_value());
    CHECK_FALSE(parse_retry_after("-5").has_value());
    CHECK_FALSE(parse_retry_after("1234567890123").has_value());
}
