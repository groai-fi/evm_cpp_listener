#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <memory>
#include <sstream>
#include <string>

#include <boost/asio.hpp>
#include <spdlog/sinks/ostream_sink.h>

#include "core/log.hpp"
#include "mock_http_server.hpp"
#include "ops/deadman.hpp"

using evml::ops::AlertClock;
using evml::ops::Deadman;
using evml::ops::DeadmanFacts;
using evml::ops::DeadmanOptions;
using evml::test::MockHttpServer;
using evml::test::MockReply;
using evml::test::MockRequest;
using Catch::Matchers::ContainsSubstring;

namespace net = boost::asio;
using tcp = net::ip::tcp;
using namespace std::chrono_literals;

namespace {

struct LogCapture {
    std::ostringstream os;
    std::shared_ptr<spdlog::logger> logger =
        evml::core::log::make_logger(std::make_shared<spdlog::sinks::ostream_sink_mt>(os), "info");
    std::string text() {
        logger->flush();
        return os.str();
    }
};

DeadmanOptions options(const std::string& url, LogCapture& cap) {
    DeadmanOptions o;
    o.url = url;
    o.user_agent = "evml-test/1";
    o.timeout = 2000ms;
    o.logger = cap.logger;
    return o;
}

template <class Pred>
void drive(net::io_context& ioc, Pred done) {
    ioc.restart();
    const auto deadline = std::chrono::steady_clock::now() + 10s;
    while (!done()) {
        if (std::chrono::steady_clock::now() > deadline) FAIL("deadman test timed out");
        ioc.run_one_for(10ms);
    }
}

// Lets the io_context run for a while so a wrongly started ping would have reached the server.
void settle(net::io_context& ioc) {
    ioc.restart();
    const auto end = std::chrono::steady_clock::now() + 150ms;
    while (std::chrono::steady_clock::now() < end) ioc.run_one_for(10ms);
}

const AlertClock::time_point t0 = AlertClock::time_point{} + 1000s;

DeadmanFacts healthy_leader(AlertClock::time_point now = t0) {
    DeadmanFacts f;
    f.leader = true;
    f.state_ok = true;
    f.c_s = 100;
    f.g_s = 90;
    f.now = now;
    return f;
}

}  // namespace

TEST_CASE("deadman: a healthy leader sends a GET with the configured User-Agent", "[deadman][integration]") {
    net::io_context ioc;
    LogCapture cap;
    MockRequest seen;
    MockHttpServer srv(ioc, [&](const MockRequest& r) {
        seen = r;
        return MockReply{200, "OK"};
    });
    Deadman d(ioc.get_executor(), options(srv.url() + "ping/uuid", cap));
    CHECK(d.enabled());
    CHECK(std::string(d.status()) == "enabled");
    CHECK(d.tick(healthy_leader()));
    drive(ioc, [&] { return d.pings_total() == 1; });

    CHECK(seen.method == "GET");
    CHECK(seen.target == "/ping/uuid");
    CHECK(seen.user_agent == "evml-test/1");
    CHECK(d.ping_failures_total() == 0);
}

TEST_CASE("deadman: a standby, a bad state or a saturated sink never ping", "[deadman][integration]") {
    net::io_context ioc;
    LogCapture cap;
    MockHttpServer srv(ioc, [&](const MockRequest&) { return MockReply{200, "OK"}; });
    Deadman d(ioc.get_executor(), options(srv.url(), cap));

    auto standby = healthy_leader();
    standby.leader = false;
    CHECK_FALSE(d.tick(standby));

    auto bad_state = healthy_leader();
    bad_state.state_ok = false;
    CHECK_FALSE(d.tick(bad_state));

    auto saturated = healthy_leader();
    saturated.sink_saturated = true;
    CHECK_FALSE(d.tick(saturated));

    settle(ioc);
    CHECK(srv.requests() == 0);
}

TEST_CASE("deadman: C_s stalled for 10 min while G_s moves stops the pings; a quiet chain keeps them",
          "[deadman][integration]") {
    net::io_context ioc;
    LogCapture cap;
    MockHttpServer srv(ioc, [&](const MockRequest&) { return MockReply{200, "OK"}; });
    Deadman d(ioc.get_executor(), options(srv.url(), cap));

    // Start tracking: C_s = 100, G_s = 90 at t0.
    REQUIRE(d.tick(healthy_leader(t0)));
    drive(ioc, [&] { return d.pings_total() == 1; });

    // 5 min later: C_s same, G_s moved. Still inside the window: healthy.
    auto f = healthy_leader(t0 + 300s);
    f.g_s = 95;
    REQUIRE(d.tick(f));
    drive(ioc, [&] { return d.pings_total() == 2; });

    // 11 min after C_s last changed, G_s moved 6 min ago: C_s stale and G_s recent -> unhealthy.
    f = healthy_leader(t0 + 660s);
    f.g_s = 96;
    CHECK_FALSE(d.tick(f));

    // Exactly the window is not yet stale (600 s, not more); a fresh object gives a clean boundary check.
    Deadman d2(ioc.get_executor(), options(srv.url(), cap));
    REQUIRE(d2.tick(healthy_leader(t0)));
    drive(ioc, [&] { return d2.pings_total() == 1; });
    auto g = healthy_leader(t0 + 600s);
    g.g_s = 91;
    CHECK(d2.tick(g));
    drive(ioc, [&] { return d2.pings_total() == 2; });

    // Quiet chain: neither C_s nor G_s moved for 11 min -> healthy.
    Deadman d3(ioc.get_executor(), options(srv.url(), cap));
    REQUIRE(d3.tick(healthy_leader(t0)));
    drive(ioc, [&] { return d3.pings_total() == 1; });
    CHECK(d3.tick(healthy_leader(t0 + 660s)));
    drive(ioc, [&] { return d3.pings_total() == 2; });
}

TEST_CASE("deadman: C_s advancing again restores the pings", "[deadman][integration]") {
    net::io_context ioc;
    LogCapture cap;
    MockHttpServer srv(ioc, [&](const MockRequest&) { return MockReply{200, "OK"}; });
    Deadman d(ioc.get_executor(), options(srv.url(), cap));
    REQUIRE(d.tick(healthy_leader(t0)));
    drive(ioc, [&] { return d.pings_total() == 1; });

    auto stalled = healthy_leader(t0 + 700s);
    stalled.g_s = 99;
    CHECK_FALSE(d.tick(stalled));

    auto advanced = healthy_leader(t0 + 710s);
    advanced.g_s = 99;
    advanced.c_s = 105;
    CHECK(d.tick(advanced));
    drive(ioc, [&] { return d.pings_total() == 2; });
}

TEST_CASE("deadman: losing leadership restarts the windows", "[deadman][integration]") {
    net::io_context ioc;
    LogCapture cap;
    MockHttpServer srv(ioc, [&](const MockRequest&) { return MockReply{200, "OK"}; });
    Deadman d(ioc.get_executor(), options(srv.url(), cap));
    REQUIRE(d.tick(healthy_leader(t0)));
    drive(ioc, [&] { return d.pings_total() == 1; });

    auto standby = healthy_leader(t0 + 100s);
    standby.leader = false;
    CHECK_FALSE(d.tick(standby));

    // Leader again 20 min later with the same C_s: the clock starts fresh, so it pings.
    auto back = healthy_leader(t0 + 1300s);
    back.g_s = 120;
    CHECK(d.tick(back));
    drive(ioc, [&] { return d.pings_total() == 2; });
}

TEST_CASE("deadman: a non-2xx answer is counted and logged without the URL", "[deadman][integration]") {
    net::io_context ioc;
    LogCapture cap;
    MockHttpServer srv(ioc, [&](const MockRequest&) { return MockReply{500, "no"}; });
    Deadman d(ioc.get_executor(), options(srv.url() + "ping/secret-uuid", cap));
    REQUIRE(d.tick(healthy_leader()));
    drive(ioc, [&] { return d.ping_failures_total() == 1; });

    CHECK(d.pings_total() == 0);
    const auto logs = cap.text();
    CHECK_THAT(logs, ContainsSubstring("dead-man ping failed"));
    CHECK_THAT(logs, ContainsSubstring("http status 500"));
    CHECK_THAT(logs, !ContainsSubstring("secret-uuid"));
    CHECK_THAT(logs, !ContainsSubstring(srv.url()));
}

TEST_CASE("deadman: a transport failure is counted and leaks no URL", "[deadman][integration]") {
    net::io_context ioc;
    LogCapture cap;
    std::string url;
    {
        tcp::acceptor a(ioc, tcp::endpoint(net::ip::make_address("127.0.0.1"), 0));
        url = "http://127.0.0.1:" + std::to_string(a.local_endpoint().port()) + "/ping/secret-uuid";
    }
    Deadman d(ioc.get_executor(), options(url, cap));
    REQUIRE(d.tick(healthy_leader()));
    drive(ioc, [&] { return d.ping_failures_total() == 1; });
    CHECK_THAT(cap.text(), !ContainsSubstring("secret-uuid"));
}

TEST_CASE("deadman: only one ping is in flight at a time", "[deadman][integration]") {
    net::io_context ioc;
    LogCapture cap;
    MockHttpServer srv(ioc, [&](const MockRequest&) { return MockReply{.status = 200, .body = "OK", .delay = 300ms}; });
    Deadman d(ioc.get_executor(), options(srv.url(), cap));
    CHECK(d.tick(healthy_leader()));
    CHECK_FALSE(d.tick(healthy_leader()));
    drive(ioc, [&] { return d.pings_total() == 1; });
    CHECK(srv.requests() == 1);
    CHECK(d.tick(healthy_leader()));  // free again
    drive(ioc, [&] { return d.pings_total() == 2; });
}

TEST_CASE("deadman: an empty URL is disabled, warns at startup and never pings", "[deadman]") {
    net::io_context ioc;
    LogCapture cap;
    Deadman d(ioc.get_executor(), options("", cap));
    CHECK_FALSE(d.enabled());
    CHECK(std::string(d.status()) == "disabled");
    CHECK_FALSE(d.tick(healthy_leader()));
    CHECK(d.ping_failures_total() == 0);
    CHECK_THAT(cap.text(), ContainsSubstring("DEADMAN_PING_URL is empty"));
}
