#include <catch2/catch_test_macros.hpp>

#include <stdexcept>

#include "chain/reorg_detector.hpp"

using namespace evml::chain;
using evml::core::Hash32;

namespace {

// Synthetic chains: branch 0 is the original chain; branch b>0 forks off branch 0 after block `fork`.
Hash32 hash_for(std::uint64_t n, std::uint8_t branch) {
    Hash32 h;
    h.bytes[0] = branch;
    for (int i = 0; i < 8; ++i) h.bytes[1 + i] = static_cast<std::uint8_t>(n >> (8 * i));
    return h;
}

struct Branch {
    std::uint8_t id;
    std::uint64_t fork;  // blocks <= fork belong to branch 0

    std::uint8_t branch_of(std::uint64_t n) const { return n <= fork ? 0 : id; }
    BlockRef at(std::uint64_t n) const {
        return BlockRef{n, hash_for(n, branch_of(n)), hash_for(n - 1, branch_of(n - 1))};
    }
};

const Branch kMain{0, 0};

void seed(ReorgDetector& d, std::uint64_t from, std::uint64_t to) {
    for (std::uint64_t n = from; n <= to; ++n) d.on_head(kMain.at(n));
}

// Drives a walk to completion fetching from `chain`; returns the number of fetches.
std::size_t run_walk(ForkWalk& w, const Branch& chain) {
    std::size_t fetches = 0;
    while (w.state() == ForkWalk::State::NeedBlock) {
        const auto b = chain.at(w.need());
        REQUIRE(b.hash == w.expected_hash());
        w.feed(b);
        ++fetches;
    }
    return fetches;
}

}  // namespace

TEST_CASE("reorg: seeds, extends and ignores duplicates", "[unit][reorg]") {
    ReorgDetector d(16);
    CHECK(d.on_head(kMain.at(10)) == HeadKind::Seeded);
    CHECK(d.on_head(kMain.at(11)) == HeadKind::Extended);
    CHECK(d.on_head(kMain.at(11)) == HeadKind::Duplicate);
    CHECK(d.on_head(kMain.at(10)) == HeadKind::Duplicate);
    CHECK(*d.top() == 11);
    CHECK(d.size() == 2);
}

TEST_CASE("reorg: the ring evicts the lowest blocks beyond capacity", "[unit][reorg]") {
    ReorgDetector d(4);
    seed(d, 1, 10);
    CHECK(d.size() == 4);
    CHECK(*d.top() == 10);
    CHECK_FALSE(d.hash_at(6).has_value());
    CHECK(d.hash_at(7).has_value());
}

TEST_CASE("reorg: zero capacity is rejected", "[unit][reorg]") {
    CHECK_THROWS_AS(ReorgDetector(0), std::invalid_argument);
}

TEST_CASE("reorg: depth-1 reorg at the same height", "[unit][reorg]") {
    ReorgDetector d(128);
    seed(d, 1, 100);
    const Branch alt{1, 99};  // block 100 replaced
    REQUIRE(d.on_head(alt.at(100)) == HeadKind::NeedWalk);
    auto w = d.begin_walk(alt.at(100));
    CHECK(run_walk(w, alt) == 0);  // parent (99) is already in the ring
    REQUIRE(w.state() == ForkWalk::State::Done);
    CHECK(w.fork_point() == 99);

    const auto info = d.apply(w);
    CHECK(info.fork_point == 99);
    CHECK(info.depth == 1);
    CHECK(info.old_top == 100);
    CHECK(*d.hash_at(100) == alt.at(100).hash);
    CHECK(*d.hash_at(99) == kMain.at(99).hash);
    CHECK_FALSE(ReorgDetector::crosses_safe(info, 90));  // above C_s: an unsafe reorg
}

TEST_CASE("reorg: depth-3 reorg where the new chain is one block longer", "[unit][reorg]") {
    ReorgDetector d(128);
    seed(d, 1, 100);
    const Branch alt{1, 97};  // blocks 98..100 replaced, plus a new 101
    REQUIRE(d.on_head(alt.at(101)) == HeadKind::NeedWalk);
    auto w = d.begin_walk(alt.at(101));
    CHECK(run_walk(w, alt) == 3);  // fetch 100, 99, 98; 97 is in the ring
    REQUIRE(w.state() == ForkWalk::State::Done);
    CHECK(w.fork_point() == 97);
    REQUIRE(w.new_chain().size() == 4);
    CHECK(w.new_chain().front().number == 98);
    CHECK(w.new_chain().back().number == 101);

    const auto info = d.apply(w);
    CHECK(info.depth == 3);
    CHECK(*d.top() == 101);
    for (std::uint64_t n = 98; n <= 101; ++n) CHECK(*d.hash_at(n) == alt.at(n).hash);
    CHECK(*d.hash_at(97) == kMain.at(97).hash);
}

TEST_CASE("reorg: depth-64 reorg is resolved inside a 128-block ring", "[unit][reorg]") {
    ReorgDetector d(128);
    seed(d, 1, 200);
    const Branch alt{1, 136};  // blocks 137..200 replaced
    REQUIRE(d.on_head(alt.at(201)) == HeadKind::NeedWalk);
    auto w = d.begin_walk(alt.at(201));
    CHECK(run_walk(w, alt) == 64);
    REQUIRE(w.state() == ForkWalk::State::Done);

    const auto info = d.apply(w);
    CHECK(info.fork_point == 136);
    CHECK(info.depth == 64);
    CHECK(d.size() == 128);
    CHECK(*d.top() == 201);
}

TEST_CASE("reorg: crossing C_s is flagged as a safe reorg", "[unit][reorg]") {
    ReorgDetector d(128);
    seed(d, 1, 100);
    const Branch alt{1, 89};
    REQUIRE(d.on_head(alt.at(101)) == HeadKind::NeedWalk);
    auto w = d.begin_walk(alt.at(101));
    run_walk(w, alt);
    REQUIRE(w.state() == ForkWalk::State::Done);
    const auto info = d.apply(w);
    CHECK(info.fork_point == 89);
    CHECK(ReorgDetector::crosses_safe(info, 95));        // released up to 95: blocks 90..95 changed
    CHECK_FALSE(ReorgDetector::crosses_safe(info, 89));  // C_s == F: nothing released was affected
    CHECK_FALSE(ReorgDetector::crosses_safe(info, 80));
}

TEST_CASE("reorg: a gap above the top is filled by a depth-0 walk", "[unit][reorg]") {
    ReorgDetector d(128);
    seed(d, 1, 100);
    REQUIRE(d.on_head(kMain.at(104)) == HeadKind::NeedWalk);
    auto w = d.begin_walk(kMain.at(104));
    CHECK(run_walk(w, kMain) == 3);  // 103, 102, 101
    REQUIRE(w.state() == ForkWalk::State::Done);
    const auto info = d.apply(w);
    CHECK(info.fork_point == 100);
    CHECK(info.depth == 0);
    CHECK(*d.top() == 104);
    CHECK(d.size() == 104);
}

TEST_CASE("reorg: a parent mismatch at top+1 triggers a walk", "[unit][reorg]") {
    ReorgDetector d(128);
    seed(d, 1, 100);
    const Branch alt{1, 98};  // 99, 100 replaced
    REQUIRE(d.on_head(alt.at(101)) == HeadKind::NeedWalk);
    // the ring must be untouched until apply()
    CHECK(*d.hash_at(100) == kMain.at(100).hash);
    CHECK(*d.top() == 100);
}

TEST_CASE("reorg: a reorg deeper than the ring ends in BeyondRing", "[unit][reorg]") {
    ReorgDetector d(8);
    seed(d, 1, 100);  // ring holds 93..100
    const Branch alt{1, 80};
    REQUIRE(d.on_head(alt.at(101)) == HeadKind::NeedWalk);
    auto w = d.begin_walk(alt.at(101));
    run_walk(w, alt);
    CHECK(w.state() == ForkWalk::State::BeyondRing);
    CHECK_THROWS_AS(d.apply(w), std::logic_error);
    CHECK(*d.top() == 100);  // untouched
}

TEST_CASE("reorg: a walk on an empty ring cannot resolve", "[unit][reorg]") {
    ReorgDetector d(8);
    const auto w = d.begin_walk(kMain.at(50));
    CHECK(w.state() == ForkWalk::State::BeyondRing);
}

TEST_CASE("reorg: a fetched block with the wrong hash makes the walk Inconsistent", "[unit][reorg]") {
    ReorgDetector d(128);
    seed(d, 1, 100);
    const Branch alt{1, 90};
    const Branch other{2, 90};  // a third chain: the provider answers from a different fork
    auto w = d.begin_walk(alt.at(101));
    REQUIRE(w.state() == ForkWalk::State::NeedBlock);
    w.feed(other.at(w.need()));
    CHECK(w.state() == ForkWalk::State::Inconsistent);
    CHECK_THROWS_AS(w.feed(alt.at(100)), std::logic_error);
    CHECK_THROWS_AS(d.apply(w), std::logic_error);
}

TEST_CASE("reorg: a block with the right hash but the wrong number is Inconsistent", "[unit][reorg]") {
    ReorgDetector d(128);
    seed(d, 1, 100);
    const Branch alt{1, 90};
    auto w = d.begin_walk(alt.at(101));
    auto wrong = alt.at(w.need());
    wrong.number += 1;
    w.feed(wrong);
    CHECK(w.state() == ForkWalk::State::Inconsistent);
}

TEST_CASE("reorg: apply rejects a walk begun before the ring changed", "[unit][reorg]") {
    ReorgDetector d(128);
    seed(d, 1, 100);
    const Branch alt{1, 99};
    auto w = d.begin_walk(alt.at(100));
    REQUIRE(w.state() == ForkWalk::State::Done);
    d.on_head(kMain.at(101));  // ring moved on
    CHECK_THROWS_AS(d.apply(w), std::logic_error);
}

TEST_CASE("reorg: reset clears the ring for a failover re-seed", "[unit][reorg]") {
    ReorgDetector d(8);
    seed(d, 1, 5);
    d.reset();
    CHECK(d.size() == 0);
    CHECK_FALSE(d.top().has_value());
    CHECK(d.on_head(kMain.at(50)) == HeadKind::Seeded);
}

TEST_CASE("reorg: timestamp_of returns non-zero timestamp only when number and hash match", "[unit][reorg]") {
    ReorgDetector d(16);
    BlockRef b99 = kMain.at(99);
    b99.timestamp = 0;  // unknown timestamp treated as a miss
    BlockRef b100 = kMain.at(100);
    b100.timestamp = 1700000100ULL;

    REQUIRE(d.on_head(b99) == HeadKind::Seeded);
    REQUIRE(d.on_head(b100) == HeadKind::Extended);

    // Hit: matching number and hash with non-zero timestamp
    CHECK(d.timestamp_of(100, b100.hash) == std::optional<std::uint64_t>{1700000100ULL});

    // Misses: timestamp == 0, wrong hash, unknown block number
    CHECK_FALSE(d.timestamp_of(99, b99.hash).has_value());
    CHECK_FALSE(d.timestamp_of(100, hash_for(100, 1)).has_value());
    CHECK_FALSE(d.timestamp_of(101, hash_for(101, 0)).has_value());

    // Post-reorg replacement at height 100
    const Branch alt{1, 99};
    BlockRef alt100 = alt.at(100);
    alt100.timestamp = 1700000202ULL;
    REQUIRE(d.on_head(alt100) == HeadKind::NeedWalk);
    auto w = d.begin_walk(alt100);
    REQUIRE(w.state() == ForkWalk::State::Done);
    d.apply(w);

    CHECK_FALSE(d.timestamp_of(100, b100.hash).has_value());
    CHECK(d.timestamp_of(100, alt100.hash) == std::optional<std::uint64_t>{1700000202ULL});
}

