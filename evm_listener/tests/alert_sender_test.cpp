#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include <boost/asio.hpp>
#include <boost/json.hpp>
#include <spdlog/sinks/ostream_sink.h>

#include "core/log.hpp"
#include "mock_http_server.hpp"
#include "ops/alert_sender.hpp"

using evml::ops::Alert;
using evml::ops::AlertSender;
using evml::ops::AlertSenderOptions;
using evml::ops::Severity;
using evml::test::MockHttpServer;
using evml::test::MockReply;
using evml::test::MockRequest;
using Catch::Matchers::ContainsSubstring;

namespace net = boost::asio;
namespace json = boost::json;
using tcp = net::ip::tcp;

namespace {

constexpr const char* kToken = "s3cr3t-token-xyz";

std::string header_of(const MockRequest& r, const std::string& name) {
    for (const auto& [k, v] : r.headers)
        if (k.size() == name.size() && std::equal(k.begin(), k.end(), name.begin(), [](char a, char b) {
                return std::tolower(static_cast<unsigned char>(a)) == std::tolower(static_cast<unsigned char>(b));
            }))
            return v;
    return "";
}

// Captures what the sender logs, so a test can prove the secrets never appear there.
struct LogCapture {
    std::ostringstream os;
    std::shared_ptr<spdlog::logger> logger =
        evml::core::log::make_logger(std::make_shared<spdlog::sinks::ostream_sink_mt>(os), "info");
    std::string text() {
        logger->flush();
        return os.str();
    }
};

AlertSenderOptions options(const std::string& url, LogCapture& cap) {
    AlertSenderOptions o;
    o.url = url;
    o.token = kToken;
    o.service = "evm-listener";
    o.shard = "base-0";
    o.host = "host-a";
    o.user_agent = "evml-test/1";
    o.backoff_base = std::chrono::milliseconds(1);
    o.backoff_cap = std::chrono::milliseconds(5);
    o.http_timeout = std::chrono::milliseconds(2000);
    o.logger = cap.logger;
    return o;
}

// Drives the single-threaded io_context until `done()`; fails the test instead of hanging.
template <class Pred>
void drive(net::io_context& ioc, Pred done) {
    ioc.restart();
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (!done()) {
        if (std::chrono::steady_clock::now() > deadline) FAIL("alert sender test timed out");
        ioc.run_one_for(std::chrono::milliseconds(10));
    }
}

Alert sample_alert() {
    Alert a;
    a.severity = Severity::Critical;
    a.title = "Head Stalled";
    a.message = "no new head for 45s";
    a.details = {{"stall_ms", 45000}, {"provider", "alchemy"}};
    return a;
}

}  // namespace

TEST_CASE("alert sender: posts the §5.3.1 schema with the X-API-Key header", "[alert_sender][integration]") {
    net::io_context ioc;
    LogCapture cap;
    MockRequest seen;
    MockHttpServer srv(ioc, [&](const MockRequest& r) {
        seen = r;
        return MockReply{200, "{}"};
    });
    AlertSender sender(ioc.get_executor(), options(srv.url() + "hook", cap));
    sender.send(sample_alert());
    drive(ioc, [&] { return sender.sent_total() == 1; });

    CHECK(seen.method == "POST");
    CHECK(seen.target == "/hook");
    CHECK(header_of(seen, "Content-Type") == "application/json");
    CHECK(header_of(seen, "X-API-Key") == kToken);
    CHECK(header_of(seen, "User-Agent") == "evml-test/1");

    const auto body = json::parse(seen.body).as_object();
    CHECK(body.at("severity").as_string() == "critical");
    CHECK(body.at("service").as_string() == "evm-listener");
    CHECK(body.at("shard").as_string() == "base-0");
    CHECK(body.at("host").as_string() == "host-a");
    CHECK(body.at("title").as_string() == "Head Stalled");
    CHECK(body.at("message").as_string() == "no new head for 45s");
    CHECK(body.at("details").as_object().at("stall_ms").as_int64() == 45000);
    CHECK(body.at("details").as_object().at("provider").as_string() == "alchemy");
    // ts: YYYY-MM-DDTHH:MM:SSZ
    const std::string ts(body.at("ts").as_string());
    REQUIRE(ts.size() == 20);
    CHECK(ts[4] == '-');
    CHECK(ts[10] == 'T');
    CHECK(ts[13] == ':');
    CHECK(ts.back() == 'Z');
    CHECK(sender.send_failures_total() == 0);
}

TEST_CASE("alert sender: a resolve is sent as severity info with the RESOLVED: title", "[alert_sender][integration]") {
    net::io_context ioc;
    LogCapture cap;
    MockRequest seen;
    MockHttpServer srv(ioc, [&](const MockRequest& r) {
        seen = r;
        return MockReply{200, "{}"};
    });
    AlertSender sender(ioc.get_executor(), options(srv.url(), cap));
    Alert a;
    a.severity = Severity::Info;
    a.title = "RESOLVED: Head Stalled";
    a.resolved = true;
    sender.send(a);
    drive(ioc, [&] { return sender.sent_total() == 1; });

    const auto body = json::parse(seen.body).as_object();
    CHECK(body.at("severity").as_string() == "info");
    CHECK(body.at("title").as_string() == "RESOLVED: Head Stalled");
}

TEST_CASE("alert sender: retries a non-2xx answer and succeeds on the third try", "[alert_sender][integration]") {
    net::io_context ioc;
    LogCapture cap;
    int n = 0;
    MockHttpServer srv(ioc, [&](const MockRequest&) { return ++n < 3 ? MockReply{500, "no"} : MockReply{200, "{}"}; });
    AlertSender sender(ioc.get_executor(), options(srv.url(), cap));
    sender.send(sample_alert());
    drive(ioc, [&] { return sender.sent_total() == 1; });

    CHECK(srv.requests() == 3);
    CHECK(sender.send_failures_total() == 0);
    const auto logs = cap.text();
    CHECK_THAT(logs, ContainsSubstring("alert delivery attempt failed"));
    CHECK_THAT(logs, ContainsSubstring("http status 500"));
}

TEST_CASE("alert sender: gives up after 1 + 3 attempts, counts one failure, logs one ERROR, leaks no secret",
          "[alert_sender][integration]") {
    net::io_context ioc;
    LogCapture cap;
    MockHttpServer srv(ioc, [&](const MockRequest&) { return MockReply{503, "down"}; });
    const std::string url = srv.url() + "hook/path-secret";
    AlertSender sender(ioc.get_executor(), options(url, cap));
    sender.send(sample_alert());
    drive(ioc, [&] { return sender.send_failures_total() == 1; });

    CHECK(srv.requests() == 4);
    CHECK(sender.sent_total() == 0);
    const auto logs = cap.text();
    CHECK_THAT(logs, ContainsSubstring("alert delivery failed, giving up"));
    CHECK_THAT(logs, ContainsSubstring("\"error\""));
    CHECK_THAT(logs, !ContainsSubstring(kToken));
    CHECK_THAT(logs, !ContainsSubstring("path-secret"));
    CHECK_THAT(logs, !ContainsSubstring(srv.url()));
}

TEST_CASE("alert sender: a transport failure is counted and leaks no secret", "[alert_sender][integration]") {
    net::io_context ioc;
    LogCapture cap;
    // A port that was just free and is closed again: connect is refused.
    std::string url;
    {
        tcp::acceptor a(ioc, tcp::endpoint(net::ip::make_address("127.0.0.1"), 0));
        url = "http://127.0.0.1:" + std::to_string(a.local_endpoint().port()) + "/path-secret";
    }
    AlertSender sender(ioc.get_executor(), options(url, cap));
    sender.send(sample_alert());
    drive(ioc, [&] { return sender.send_failures_total() == 1; });

    const auto logs = cap.text();
    CHECK_THAT(logs, ContainsSubstring("alert delivery failed, giving up"));
    CHECK_THAT(logs, !ContainsSubstring(kToken));
    CHECK_THAT(logs, !ContainsSubstring("path-secret"));
}

TEST_CASE("alert sender: constructor fails fast on a missing url or token", "[alert_sender]") {
    net::io_context ioc;
    LogCapture cap;
    auto no_url = options("", cap);
    CHECK_THROWS_AS(AlertSender(ioc.get_executor(), no_url), std::invalid_argument);
    auto no_token = options("http://127.0.0.1:1/", cap);
    no_token.token.clear();
    CHECK_THROWS_AS(AlertSender(ioc.get_executor(), no_token), std::invalid_argument);
}
