#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <stdexcept>
#include <string>

#include "ops/metrics.hpp"

using evml::ops::Metrics;
using Catch::Matchers::ContainsSubstring;

namespace {
bool has_line(const std::string& text, const std::string& line) {
    return text.find("\n" + line + "\n") != std::string::npos || text.rfind(line + "\n", 0) == 0;
}
}  // namespace

TEST_CASE("metrics: a counter is brought up to the total, not incremented by it", "[metrics]") {
    Metrics m;
    m.counter_total("evml_dup_total", "dups", {}, 5);
    CHECK(has_line(m.serialize(), "evml_dup_total 5"));
    m.counter_total("evml_dup_total", "dups", {}, 8);  // +3
    CHECK(has_line(m.serialize(), "evml_dup_total 8"));
}

TEST_CASE("metrics: repeating the same total never double-counts", "[metrics]") {
    Metrics m;
    for (int i = 0; i < 5; ++i) m.counter_total("evml_dup_total", "dups", {}, 7);
    CHECK(has_line(m.serialize(), "evml_dup_total 7"));
}

TEST_CASE("metrics: a total that drops (source rebuilt) keeps the counter rising", "[metrics]") {
    Metrics m;
    m.counter_total("evml_dup_total", "dups", {}, 10);
    m.counter_total("evml_dup_total", "dups", {}, 2);  // new sink restarted at 0 and counted 2
    CHECK(has_line(m.serialize(), "evml_dup_total 12"));
    m.counter_total("evml_dup_total", "dups", {}, 5);  // +3 on top of the new baseline
    CHECK(has_line(m.serialize(), "evml_dup_total 15"));
}

TEST_CASE("metrics: label sets are separate series with their own baselines", "[metrics]") {
    Metrics m;
    m.counter_total("evml_inconsistency_total", "bad", {{"provider", "a"}}, 3);
    m.counter_total("evml_inconsistency_total", "bad", {{"provider", "b"}}, 1);
    m.counter_total("evml_inconsistency_total", "bad", {{"provider", "a"}}, 4);
    const auto out = m.serialize();
    CHECK(has_line(out, "evml_inconsistency_total{provider=\"a\"} 4"));
    CHECK(has_line(out, "evml_inconsistency_total{provider=\"b\"} 1"));
}

TEST_CASE("metrics: gauges are set to the latest value and TYPE / HELP lines are present", "[metrics]") {
    Metrics m;
    m.gauge("evml_role", "role", {{"role", "leader"}}, 1);
    m.gauge("evml_role", "role", {{"role", "leader"}}, 0);
    m.counter_total("evml_dup_total", "dups", {}, 1);
    const auto out = m.serialize();
    CHECK(has_line(out, "evml_role{role=\"leader\"} 0"));
    CHECK_THAT(out, ContainsSubstring("# TYPE evml_role gauge"));
    CHECK_THAT(out, ContainsSubstring("# HELP evml_role role"));
    CHECK_THAT(out, ContainsSubstring("# TYPE evml_dup_total counter"));
}

TEST_CASE("metrics: misuse fails loudly", "[metrics]") {
    Metrics m;
    m.counter_total("evml_x", "x", {}, 1);
    CHECK_THROWS_AS(m.gauge("evml_x", "x", {}, 1), std::logic_error);
    CHECK_THROWS_AS(m.counter_total("evml_x", "other help", {}, 2), std::logic_error);
    m.gauge("evml_y", "y", {}, 1);
    CHECK_THROWS_AS(m.counter_total("evml_y", "y", {}, 1), std::logic_error);
    CHECK_THROWS_AS(m.counter_total("bad name!", "x", {}, 1), std::invalid_argument);
}

TEST_CASE("metrics: fast-stream emitted and dropped counters serialize with Prometheus TYPE and HELP", "[metrics]") {
    Metrics m;
    m.counter_total("evml_fast_emitted_total", "Fast-stream tip events emitted to Redis", {}, 4);
    m.counter_total("evml_fast_dropped_total",
                    "Fast-stream tip events dropped due to backpressure, outage, or fencing", {}, 1);
    const auto out = m.serialize();
    CHECK(has_line(out, "evml_fast_emitted_total 4"));
    CHECK(has_line(out, "evml_fast_dropped_total 1"));
    CHECK_THAT(out, ContainsSubstring("# TYPE evml_fast_emitted_total counter"));
    CHECK_THAT(out, ContainsSubstring("# TYPE evml_fast_dropped_total counter"));
}

