#include <catch2/catch_test_macros.hpp>

#include "base_config.hpp"
#include "pipeline/event.hpp"
#include "pipeline/final_diff.hpp"

using namespace evml::pipeline;
using evml::test::hash_of;

namespace {

constexpr std::uint64_t kChain = 8453;

evml::rpc::Log log_at(std::uint64_t block, std::uint8_t hash, std::uint64_t idx) {
    evml::rpc::Log l;
    l.block_number = block;
    l.block_hash = hash_of(hash);
    l.log_index = idx;
    return l;
}

ReleasedEntry entry(std::uint64_t block, std::uint8_t hash, std::uint64_t idx) {
    return ReleasedEntry{make_event_id(kChain, hash_of(hash), idx), block, evml::core::StreamKind::Events, {}};
}

}  // namespace

TEST_CASE("diff: identical sets are all matched", "[unit][diff]") {
    const auto d = diff_released(kChain, {entry(10, 1, 0), entry(10, 1, 1)}, {log_at(10, 1, 0), log_at(10, 1, 1)});
    CHECK(d.matched.size() == 2);
    CHECK(d.late.empty());
    CHECK(d.vanished.empty());
}

TEST_CASE("diff: a sweep-only log is late", "[unit][diff]") {
    const auto d = diff_released(kChain, {entry(10, 1, 0)}, {log_at(10, 1, 0), log_at(11, 2, 0)});
    CHECK(d.matched.size() == 1);
    REQUIRE(d.late.size() == 1);
    CHECK(d.late[0].block_number == 11);
    CHECK(d.vanished.empty());
}

TEST_CASE("diff: a released-only entry vanished", "[unit][diff]") {
    const auto d = diff_released(kChain, {entry(10, 1, 0), entry(12, 3, 0)}, {log_at(10, 1, 0)});
    CHECK(d.matched.size() == 1);
    CHECK(d.late.empty());
    REQUIRE(d.vanished.size() == 1);
    CHECK(d.vanished[0].block_number == 12);
}

TEST_CASE("diff: a log re-mined under another block hash is vanished plus late", "[unit][diff]") {
    const auto d = diff_released(kChain, {entry(10, 1, 0)}, {log_at(10, 2, 0)});
    CHECK(d.matched.empty());
    REQUIRE(d.late.size() == 1);
    CHECK(d.late[0].block_hash == hash_of(2));
    REQUIRE(d.vanished.size() == 1);
    CHECK(d.vanished[0].event_id == make_event_id(kChain, hash_of(1), 0));
}

TEST_CASE("diff: vanished entries are ordered by block then id", "[unit][diff]") {
    const auto d = diff_released(kChain, {entry(12, 3, 0), entry(10, 1, 1), entry(10, 1, 0)}, {});
    REQUIRE(d.vanished.size() == 3);
    CHECK(d.vanished[0].block_number == 10);
    CHECK(d.vanished[0].event_id < d.vanished[1].event_id);
    CHECK(d.vanished[2].block_number == 12);
}

TEST_CASE("diff: empty inputs produce an empty diff", "[unit][diff]") {
    const auto d = diff_released(kChain, {}, {});
    CHECK(d.matched.empty());
    CHECK(d.late.empty());
    CHECK(d.vanished.empty());
}
