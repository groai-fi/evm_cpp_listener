// Pure judgement of the consumer-group lag probe (plan 4.10): no Redis involved. The fixture numbers are synthetic
// stream ids chosen so the expected ages are easy to check by hand.

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include "sink/consumer_lag.hpp"

using namespace evml::sink;
using evml::core::ConsumerGroupCfg;
using evml::core::StreamKind;
using Catch::Matchers::ContainsSubstring;

namespace {

constexpr std::uint64_t kNowMs = 1700000100000;

GroupFacts healthy() {
    GroupFacts f;
    f.stream_exists = true;
    f.group_found = true;
    f.last_delivered_id = "1700000000000-0";
    f.lag = 0;
    f.max_deleted_id = "0-0";
    f.first_entry_id = "1699990000000-0";
    f.now_ms = kNowMs;
    return f;
}

const ConsumerGroupCfg kCfg{StreamKind::Events, "bot"};

}  // namespace

TEST_CASE("stream id: parse and order", "[unit][sink]") {
    const auto a = StreamId::parse("1700000000000-5");
    CHECK(a.ms == 1700000000000);
    CHECK(a.seq == 5);
    CHECK(StreamId::parse("5-1") < StreamId::parse("5-2"));
    CHECK(StreamId::parse("5-9") < StreamId::parse("6-0"));
    CHECK(StreamId::parse("6-0") > StreamId::parse("5-9"));
    CHECK(StreamId::parse("0-0") == StreamId{0, 0});
    for (const char* bad : {"", "12", "-1", "1-", "a-b", "1-2-3", "1- 2"})
        CHECK_THROWS_WITH(StreamId::parse(bad), ContainsSubstring("not '<ms>-<seq>'"));
}

TEST_CASE("consumer lag: a group that is caught up reports zero ages", "[unit][sink]") {
    const auto r = evaluate_consumer_lag(kCfg, healthy());
    CHECK_FALSE(r.missing);
    CHECK(r.stream == StreamKind::Events);
    CHECK(r.group == "bot");
    CHECK(r.undelivered_age_s == 0);
    CHECK(r.pending_age_s == 0);
    CHECK(r.lag == 0);
    CHECK_FALSE(r.trimmed_unread);
}

TEST_CASE("consumer lag: ages come from the entry ids and Redis TIME", "[unit][sink]") {
    auto f = healthy();
    f.lag = 7;
    f.first_undelivered_id = "1700000000200-0";  // 99800 ms before now -> 99 s
    f.smallest_pending_id = "1699999000000-3";   // 1100000 ms before now -> 1100 s
    const auto r = evaluate_consumer_lag(kCfg, f);
    CHECK(r.undelivered_age_s == 99);
    CHECK(r.pending_age_s == 1100);
    CHECK(r.lag == 7);
    CHECK_FALSE(r.trimmed_unread);

    f.first_undelivered_id = "1700000200000-0";  // newer than Redis TIME (clock read race): never negative
    CHECK(evaluate_consumer_lag(kCfg, f).undelivered_age_s == 0);
}

TEST_CASE("consumer lag: a missing stream or group is reported as missing", "[unit][sink]") {
    GroupFacts none;
    CHECK(evaluate_consumer_lag(kCfg, none).missing);

    auto f = healthy();
    f.group_found = false;
    const auto r = evaluate_consumer_lag(kCfg, f);
    CHECK(r.missing);
    CHECK(r.group == "bot");
}

TEST_CASE("consumer lag: trimmed_unread when entries past the group cursor were deleted", "[unit][sink]") {
    auto f = healthy();
    f.max_deleted_id = "1700000000000-0";  // equal to last-delivered: everything deleted was already read
    CHECK_FALSE(evaluate_consumer_lag(kCfg, f).trimmed_unread);
    f.max_deleted_id = "1700000000000-1";
    CHECK(evaluate_consumer_lag(kCfg, f).trimmed_unread);
}

TEST_CASE("consumer lag: trimmed_unread when pending entries fell off the front of the stream", "[unit][sink]") {
    auto f = healthy();
    f.smallest_pending_id = "1699990000000-0";  // still the first entry: present
    CHECK_FALSE(evaluate_consumer_lag(kCfg, f).trimmed_unread);
    f.smallest_pending_id = "1699980000000-0";  // older than the first entry: trimmed before it was acked
    CHECK(evaluate_consumer_lag(kCfg, f).trimmed_unread);
}

TEST_CASE("consumer lag: an unparsable stream id fails loudly", "[unit][sink]") {
    auto f = healthy();
    f.last_delivered_id = "garbage";
    CHECK_THROWS_WITH(evaluate_consumer_lag(kCfg, f), ContainsSubstring("garbage"));
}

TEST_CASE("consumer lag: trimmed_unread from the counters when nothing is pending and max-deleted-entry-id is 0-0", "[unit][sink]") {
    auto f = healthy();
    f.entries_added = 5;
    f.entries_read = 2;
    f.lag = 3;  // 2 read + 3 waiting = 5 added: nothing lost
    CHECK_FALSE(evaluate_consumer_lag(kCfg, f).trimmed_unread);
    f.lag = 1;  // 2 read + 1 waiting < 5 added: 2 entries were removed unread
    CHECK(evaluate_consumer_lag(kCfg, f).trimmed_unread);
    f.entries_read.reset();  // Redis does not know what the group read: not provable, not flagged
    CHECK_FALSE(evaluate_consumer_lag(kCfg, f).trimmed_unread);
    f.entries_read = 2;
    f.lag.reset();
    CHECK_FALSE(evaluate_consumer_lag(kCfg, f).trimmed_unread);
}

TEST_CASE("consumer lag: a group that never delivered anything has read zero entries", "[unit][sink]") {
    auto f = healthy();
    f.last_delivered_id = "0-0";
    f.entries_read.reset();  // Redis reports nil for a group created at 0 that never read
    f.entries_added = 5;
    f.lag = 1;
    CHECK(evaluate_consumer_lag(kCfg, f).trimmed_unread);  // 0 read + 1 waiting < 5 added
    f.lag = 5;
    CHECK_FALSE(evaluate_consumer_lag(kCfg, f).trimmed_unread);
}
