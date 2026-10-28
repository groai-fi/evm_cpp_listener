#include <catch2/catch_test_macros.hpp>

#include "provider/provider_book.hpp"

using namespace evml::provider;
using evml::rpc::FailKind;
using namespace std::chrono_literals;

namespace {
const TimePoint T0{};  // fake clock origin
TimePoint at(std::chrono::seconds s) { return T0 + s; }

ProviderBook book() {
    // WS: quicknode (preferred) -> infura.  getLogs: infura -> alchemy -> ankr -> public -> quicknode.
    return ProviderBook({"quicknode", "infura"}, {"infura", "alchemy", "ankr", "public", "quicknode"});
}

void fail_n(ProviderBook& b, const char* name, int n, TimePoint now, FailKind k = FailKind::Retry) {
    for (int i = 0; i < n; ++i) b.on_failure(name, k, std::nullopt, now);
}
}  // namespace

TEST_CASE("book: construction validates input") {
    CHECK_THROWS_AS(ProviderBook({}, {}), std::invalid_argument);
    CHECK_THROWS_AS(ProviderBook({"a", "a"}, {"b"}), std::invalid_argument);
    CHECK_THROWS_AS(ProviderBook({"a"}, {"b", "b"}), std::invalid_argument);
    CHECK_NOTHROW(ProviderBook({"a"}, {"a"}));
}

TEST_CASE("book: unknown provider names fail loudly") {
    auto b = book();
    CHECK_THROWS_AS(b.state("nope"), std::out_of_range);
    CHECK_THROWS_AS(b.on_success("nope", T0), std::out_of_range);
    CHECK_THROWS_AS(b.usable("nope", T0), std::out_of_range);
}

TEST_CASE("book: transient failures go Down only at the 3rd consecutive one; success resets") {
    auto b = book();
    CHECK_FALSE(b.on_failure("infura", FailKind::Retry, std::nullopt, at(0s)).has_value());
    CHECK_FALSE(b.on_failure("infura", FailKind::Retry, std::nullopt, at(1s)).has_value());
    b.on_success("infura", at(2s));
    CHECK(b.state("infura").consecutive_failures == 0);

    fail_n(b, "infura", 2, at(3s));
    const auto t = b.on_failure("infura", FailKind::Retry, std::nullopt, at(4s));
    REQUIRE(t.has_value());
    CHECK(t->provider == "infura");
    CHECK(t->from == State::Up);
    CHECK(t->to == State::Down);
    CHECK_FALSE(b.usable("infura", at(4s)));
}

TEST_CASE("book: a Down provider is probed every 30 s and returns only on a successful probe") {
    auto b = book();
    fail_n(b, "ankr", 3, at(0s));
    CHECK_FALSE(b.probe_due("ankr", at(29s)));
    CHECK(b.probe_due("ankr", at(30s)));

    CHECK_FALSE(b.on_probe_result("ankr", false, at(30s)).has_value());  // still down
    CHECK_FALSE(b.probe_due("ankr", at(59s)));
    CHECK(b.probe_due("ankr", at(60s)));

    const auto t = b.on_probe_result("ankr", true, at(60s));
    REQUIRE(t.has_value());
    CHECK(t->to == State::Up);
    CHECK(b.usable("ankr", at(60s)));
    CHECK(b.state("ankr").healthy_since == at(60s));
}

TEST_CASE("book: QuotaExhausted -> Cooldown immediately, re-probed hourly (scenario 9)") {
    auto b = book();
    const auto t = b.on_failure("alchemy", FailKind::QuotaExhausted, std::nullopt, at(0s));
    REQUIRE(t.has_value());
    CHECK(t->to == State::Cooldown);
    CHECK_FALSE(b.probe_due("alchemy", at(3599s)));
    CHECK(b.probe_due("alchemy", at(3600s)));

    CHECK_FALSE(b.on_probe_result("alchemy", false, at(3600s)).has_value());  // still exhausted
    CHECK(b.state("alchemy").state == State::Cooldown);
    CHECK_FALSE(b.probe_due("alchemy", at(7199s)));
    CHECK(b.probe_due("alchemy", at(7200s)));

    CHECK(b.on_probe_result("alchemy", true, at(7200s)).has_value());  // recovered next billing cycle
    CHECK(b.usable("alchemy", at(7200s)));
}

TEST_CASE("book: AuthFatal -> Disabled for 10 min; ConfigFatal is permanent (scenarios 5, 6)") {
    auto b = book();
    const auto t = b.on_failure("infura", FailKind::AuthFatal, std::nullopt, at(0s));
    REQUIRE(t.has_value());
    CHECK(t->to == State::Disabled);
    CHECK_FALSE(b.probe_due("infura", at(599s)));
    CHECK(b.probe_due("infura", at(600s)));

    const auto f = b.on_failure("public", FailKind::ConfigFatal, std::nullopt, at(0s));
    REQUIRE(f.has_value());
    CHECK(f->to == State::Fatal);
    CHECK_FALSE(b.probe_due("public", at(100000s)));
    CHECK_FALSE(b.on_success("public", at(1s)).has_value());           // cannot be revived
    CHECK_FALSE(b.on_probe_result("public", true, at(1s)).has_value());
    CHECK_FALSE(b.usable("public", at(100000s)));
}

TEST_CASE("book: RateLimited keeps the provider Up but unusable until Retry-After (scenario 4)") {
    auto b = book();
    CHECK_FALSE(b.on_failure("infura", FailKind::RateLimited, 3s, at(0s)).has_value());
    CHECK(b.state("infura").state == State::Up);
    CHECK_FALSE(b.usable("infura", at(2s)));
    CHECK(b.usable("infura", at(3s)));

    // no hint -> default 10 s
    CHECK_FALSE(b.on_failure("ankr", FailKind::RateLimited, std::nullopt, at(100s)).has_value());
    CHECK_FALSE(b.usable("ankr", at(109s)));
    CHECK(b.usable("ankr", at(110s)));

    // a later, shorter hint never shortens an existing wait
    b.on_failure("ankr", FailKind::RateLimited, 1s, at(101s));
    CHECK_FALSE(b.usable("ankr", at(109s)));
}

TEST_CASE("book: RangeTooLarge is not a provider failure") {
    auto b = book();
    for (int i = 0; i < 10; ++i) CHECK_FALSE(b.on_failure("public", FailKind::RangeTooLarge, std::nullopt, at(0s)).has_value());
    CHECK(b.state("public").state == State::Up);
    CHECK(b.state("public").consecutive_failures == 0);
}

TEST_CASE("book: initial WS selection and forced failover on Down") {
    auto b = book();
    auto d = b.select_ws(at(0s));
    CHECK(d.reason == ProviderBook::WsDecision::Reason::Initial);
    CHECK_FALSE(d.from.has_value());
    CHECK(d.provider == "quicknode");
    CHECK(d.changed);

    d = b.select_ws(at(1s));
    CHECK(d.reason == ProviderBook::WsDecision::Reason::Keep);
    CHECK(d.from == "quicknode");
    CHECK_FALSE(d.changed);

    fail_n(b, "quicknode", 3, at(5s));
    d = b.select_ws(at(5s));
    CHECK(d.reason == ProviderBook::WsDecision::Reason::Forced);
    CHECK(d.from == "quicknode");
    CHECK(d.provider == "infura");
    CHECK(b.active_ws() == "infura");
}

TEST_CASE("book: quota on the active WS provider fails over immediately, ignoring the voluntary gap (scenario 9)") {
    auto b = book();
    b.select_ws(at(0s));
    b.on_failure("quicknode", FailKind::QuotaExhausted, std::nullopt, at(1s));
    const auto d = b.select_ws(at(1s));
    CHECK(d.reason == ProviderBook::WsDecision::Reason::Forced);
    CHECK(d.provider == "infura");
}

TEST_CASE("book: an active provider that is merely rate-limited is kept") {
    auto b = book();
    b.select_ws(at(0s));
    b.on_failure("quicknode", FailKind::RateLimited, 30s, at(1s));
    const auto d = b.select_ws(at(2s));
    CHECK(d.reason == ProviderBook::WsDecision::Reason::Keep);
    CHECK(d.provider == "quicknode");
}

TEST_CASE("book: switch back only after the preferred provider was healthy for 10 min (scenario 10)") {
    auto b = book();
    b.select_ws(at(0s));
    fail_n(b, "quicknode", 3, at(10s));
    REQUIRE(b.select_ws(at(10s)).provider == "infura");

    b.on_probe_result("quicknode", true, at(100s));  // healthy_since = 100 s
    CHECK(b.select_ws(at(100s + 599s)).reason == ProviderBook::WsDecision::Reason::Keep);

    const auto d = b.select_ws(at(100s + 600s));
    CHECK(d.reason == ProviderBook::WsDecision::Reason::SwitchBack);
    CHECK(d.from == "infura");
    CHECK(d.provider == "quicknode");
    CHECK(b.active_ws() == "quicknode");
}

TEST_CASE("book: alternating faults never produce a switch-back (no flapping)") {
    auto b = book();
    b.select_ws(at(0s));
    fail_n(b, "quicknode", 3, at(10s));
    REQUIRE(b.select_ws(at(10s)).provider == "infura");

    // quicknode recovers, then fails again, before 10 minutes of stability ever accumulates
    for (int cycle = 0; cycle < 6; ++cycle) {
        const auto base = 100s + cycle * 400s;
        b.on_probe_result("quicknode", true, at(base));
        CHECK(b.select_ws(at(base + 399s)).reason == ProviderBook::WsDecision::Reason::Keep);
        fail_n(b, "quicknode", 3, at(base + 399s));
    }
    CHECK(b.active_ws() == "infura");
}

TEST_CASE("book: voluntary demotion honours the 60 s spacing") {
    auto b = book();
    b.select_ws(at(0s));
    fail_n(b, "quicknode", 3, at(10s));
    REQUIRE(b.select_ws(at(10s)).provider == "infura");  // forced failover at 10 s opens the gap window

    CHECK_FALSE(b.demote_voluntary("infura", at(69s)).has_value());  // refused: inside 60 s
    CHECK(b.state("infura").state == State::Up);
    const auto t = b.demote_voluntary("infura", at(70s));
    REQUIRE(t.has_value());
    CHECK(t->to == State::Down);
    CHECK_FALSE(b.demote_voluntary("infura", at(200s)).has_value());  // already Down: nothing to do
}

TEST_CASE("book: every WS provider down -> NoneAvailable, then recovery resumes (scenario 11)") {
    auto b = book();
    b.select_ws(at(0s));
    fail_n(b, "quicknode", 3, at(1s));
    fail_n(b, "infura", 3, at(1s));
    auto d = b.select_ws(at(1s));
    CHECK(d.reason == ProviderBook::WsDecision::Reason::NoneAvailable);
    CHECK(d.from == "quicknode");
    CHECK_FALSE(d.provider.has_value());
    CHECK_FALSE(b.active_ws().has_value());

    b.on_probe_result("infura", true, at(40s));
    d = b.select_ws(at(40s));
    CHECK_FALSE(d.from.has_value());
    CHECK(d.provider == "infura");
    CHECK(d.changed);
}

TEST_CASE("book: getLogs prefers a provider other than the active WS one, falls back to it last") {
    auto b = book();
    b.select_ws(at(0s));  // active WS = quicknode (last in the getLogs order)
    CHECK(b.pick_getlogs(at(0s)) == "infura");

    fail_n(b, "infura", 3, at(1s));
    CHECK(b.pick_getlogs(at(1s)) == "alchemy");

    b.on_failure("alchemy", FailKind::QuotaExhausted, std::nullopt, at(1s));
    b.on_failure("ankr", FailKind::AuthFatal, std::nullopt, at(1s));
    CHECK(b.pick_getlogs(at(1s)) == "public");

    fail_n(b, "public", 3, at(1s));
    CHECK(b.pick_getlogs(at(1s)) == "quicknode");  // only the active WS provider is left

    fail_n(b, "quicknode", 3, at(1s));
    CHECK_FALSE(b.pick_getlogs(at(1s)).has_value());
}

TEST_CASE("book: getLogs selection skips a rate-limited provider until it is usable again") {
    auto b = book();
    b.select_ws(at(0s));
    b.on_failure("infura", FailKind::RateLimited, 5s, at(0s));
    CHECK(b.pick_getlogs(at(1s)) == "alchemy");
    CHECK(b.pick_getlogs(at(5s)) == "infura");
}

TEST_CASE("book: a provider absent from an order list is not a candidate for that chain (scenario 14)") {
    ProviderBook b({"quicknode"}, {"infura", "quicknode"});  // alchemy not enabled: simply not in the lists
    CHECK_THROWS_AS(b.state("alchemy"), std::out_of_range);
    b.select_ws(at(0s));
    CHECK(b.pick_getlogs(at(0s)) == "infura");
}
