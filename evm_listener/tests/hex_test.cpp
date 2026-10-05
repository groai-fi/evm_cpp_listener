#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <unordered_set>

#include "core/error.hpp"
#include "core/hex.hpp"

using namespace evml::core;

TEST_CASE("parse_u64_hex accepts valid quantities", "[unit][hex]") {
    CHECK(parse_u64_hex("0x0") == 0);
    CHECK(parse_u64_hex("0x2a") == 42);
    CHECK(parse_u64_hex("0X2A") == 42);
    CHECK(parse_u64_hex("0x0000000000000000000001") == 1);  // leading zeros that still fit
    CHECK(parse_u64_hex("0xffffffffffffffff") == UINT64_MAX);
}

TEST_CASE("parse_u64_hex rejects malformed input", "[unit][hex]") {
    CHECK_THROWS_AS(parse_u64_hex(""), DecodeError);
    CHECK_THROWS_AS(parse_u64_hex("0x"), DecodeError);
    CHECK_THROWS_AS(parse_u64_hex("2a"), DecodeError);
    CHECK_THROWS_AS(parse_u64_hex("0xzz"), DecodeError);
    CHECK_THROWS_AS(parse_u64_hex("0x1_"), DecodeError);
    CHECK_THROWS_AS(parse_u64_hex("0x10000000000000000"), DecodeError);  // 2^64
}

TEST_CASE("to_hex_u64 is minimal lowercase and round-trips", "[unit][hex]") {
    CHECK(to_hex_u64(0) == "0x0");
    CHECK(to_hex_u64(255) == "0xff");
    CHECK(to_hex_u64(UINT64_MAX) == "0xffffffffffffffff");
    for (const std::uint64_t v : std::initializer_list<std::uint64_t>{1, 16, 1234567890123ULL, UINT64_MAX}) {
        CHECK(parse_u64_hex(to_hex_u64(v)) == v);
    }
}

TEST_CASE("Address20 / Hash32 parse strictly and round-trip lowercase", "[unit][hex]") {
    const std::string mixed = "0xc5E51044eB7318950B1aFb044FccFb25782C48c1";
    const auto a = Address20::from_hex(mixed);
    CHECK(a.to_hex() == "0xc5e51044eb7318950b1afb044fccfb25782c48c1");
    CHECK(a == Address20::from_hex(a.to_hex()));

    const auto h = Hash32::from_hex("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef");
    CHECK(h.to_hex() == "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef");

    CHECK_THROWS_AS(Address20::from_hex("c5e51044eb7318950b1afb044fccfb25782c48c1"), DecodeError);   // no 0x
    CHECK_THROWS_AS(Address20::from_hex("0xc5e51044eb7318950b1afb044fccfb25782c48"), DecodeError);   // short
    CHECK_THROWS_AS(Address20::from_hex("0xc5e51044eb7318950b1afb044fccfb25782c48c1ff"), DecodeError);  // long
    CHECK_THROWS_AS(Address20::from_hex("0xg5e51044eb7318950b1afb044fccfb25782c48c1"), DecodeError);  // bad digit
    CHECK_THROWS_AS(Hash32::from_hex("0x00"), DecodeError);
}

TEST_CASE("FixedBytes is hashable and usable in unordered containers", "[unit][hex]") {
    std::unordered_set<Hash32> set;
    const auto a = Hash32::from_hex("0x" + std::string(64, '1'));
    const auto b = Hash32::from_hex("0x" + std::string(64, '2'));
    set.insert(a);
    set.insert(a);
    set.insert(b);
    CHECK(set.size() == 2);
    CHECK(set.contains(a));
}
