#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <map>
#include <numeric>
#include <vector>

#include "rpc/backoff.hpp"

using evml::rpc::Backoff;
using ms = std::chrono::milliseconds;

TEST_CASE("backoff window doubles up to the cap") {
    Backoff b(1, ms(250), ms(30000));
    const std::vector<std::int64_t> expect{250, 500, 1000, 2000, 4000, 8000, 16000, 30000, 30000};
    for (const auto w : expect) {
        CHECK(b.window().count() == w);
        b.next();
    }
}

TEST_CASE("backoff delays are full-jitter within [0, window] and actually spread") {
    Backoff b(42, ms(250), ms(30000));
    for (int i = 0; i < 12; ++i) b.next();  // saturate at cap
    REQUIRE(b.window() == ms(30000));

    std::vector<std::int64_t> samples;
    for (int i = 0; i < 2000; ++i) {
        const auto w = b.window();
        const auto d = b.next();
        REQUIRE(d.count() >= 0);
        REQUIRE(d <= w);
        samples.push_back(d.count());
    }
    const double mean = std::accumulate(samples.begin(), samples.end(), 0.0) / static_cast<double>(samples.size());
    CHECK(mean > 13000.0);  // uniform(0,30000) -> ~15000
    CHECK(mean < 17000.0);
    CHECK(*std::min_element(samples.begin(), samples.end()) < 1000);   // uses the low end (full jitter)
    CHECK(*std::max_element(samples.begin(), samples.end()) > 29000);  // and the high end
}

TEST_CASE("backoff is deterministic per seed and differs across seeds") {
    Backoff a(7), b(7), c(8);
    std::vector<std::int64_t> da, db, dc;
    for (int i = 0; i < 20; ++i) {
        da.push_back(a.next().count());
        db.push_back(b.next().count());
        dc.push_back(c.next().count());
    }
    CHECK(da == db);
    CHECK(da != dc);
}

TEST_CASE("backoff resets only after a long enough live period") {
    Backoff b(1, ms(250), ms(30000), ms(30000));
    for (int i = 0; i < 5; ++i) b.next();
    REQUIRE(b.attempt() == 5);

    b.report_live(ms(29999));  // flapping session: no reset
    CHECK(b.attempt() == 5);

    b.report_live(ms(30000));
    CHECK(b.attempt() == 0);
    CHECK(b.window() == ms(250));
}

TEST_CASE("backoff never overflows for huge attempt counts") {
    Backoff b(1, ms(250), ms(30000));
    for (int i = 0; i < 100000; ++i) b.next();
    CHECK(b.window() == ms(30000));
}

TEST_CASE("backoff rejects invalid parameters") {
    CHECK_THROWS_AS(Backoff(1, ms(0), ms(10)), std::invalid_argument);
    CHECK_THROWS_AS(Backoff(1, ms(100), ms(50)), std::invalid_argument);
}
