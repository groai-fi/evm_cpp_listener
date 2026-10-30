#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include "sink/keys.hpp"

using evml::core::StreamKind;
using evml::sink::Keys;
using Catch::Matchers::ContainsSubstring;

TEST_CASE("keys: shard-scoped Redis keys share the {shard} hash tag including fast stream", "[unit][config][keys]") {
    const Keys k("base_eurc_usdc");
    CHECK(k.events() == "evml:{base_eurc_usdc}:events");
    CHECK(k.registry() == "evml:{base_eurc_usdc}:registry");
    CHECK(k.fast() == "evml:{base_eurc_usdc}:fast");
    CHECK(k.stream(StreamKind::Events) == "evml:{base_eurc_usdc}:events");
    CHECK(k.stream(StreamKind::Registry) == "evml:{base_eurc_usdc}:registry");
    CHECK(k.ckpt() == "evml:{base_eurc_usdc}:ckpt");
    CHECK(k.released() == "evml:{base_eurc_usdc}:released");
    CHECK(k.released_data() == "evml:{base_eurc_usdc}:released_data");
    CHECK(k.leader() == "evml:{base_eurc_usdc}:leader");
    CHECK(k.epoch() == "evml:{base_eurc_usdc}:epoch");
    CHECK(k.hb("host-a") == "evml:{base_eurc_usdc}:hb:host-a");
    CHECK(k.dedup("dedup:8453:0xabc:0") == "evml:{base_eurc_usdc}:dedup:8453:0xabc:0");
    CHECK(k.pattern() == "evml:{base_eurc_usdc}:*");

    CHECK_THROWS_WITH(Keys(""), ContainsSubstring("non-empty"));
    CHECK_THROWS_WITH(Keys("bad{shard}"), ContainsSubstring("no braces"));
}

