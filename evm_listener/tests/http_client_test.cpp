#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <future>
#include <stdexcept>
#include <string>

#include <boost/asio.hpp>

#include "mock_http_server.hpp"
#include "ops/http_client.hpp"
#include "rpc/blocking_http.hpp"

using evml::ops::HttpClient;
using evml::ops::HttpClientOptions;
using evml::ops::HttpResponse;
using evml::test::MockHttpServer;
using evml::test::MockReply;
using evml::test::MockRequest;
using Catch::Matchers::ContainsSubstring;

namespace net = boost::asio;
using tcp = net::ip::tcp;

namespace {

constexpr const char* kSecret = "s3cr3t-token-xyz";

// Drives the single-threaded io_context until the coroutine finishes; fails the test instead of hanging.
template <class T>
T run(net::io_context& ioc, net::awaitable<T> aw) {
    auto fut = net::co_spawn(ioc, std::move(aw), net::use_future);
    ioc.restart();
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (fut.wait_for(std::chrono::seconds(0)) != std::future_status::ready) {
        if (std::chrono::steady_clock::now() > deadline) FAIL("http client test timed out");
        ioc.run_one_for(std::chrono::milliseconds(10));
    }
    return fut.get();
}

// The exception text of a call that must fail ("" if it did not throw).
std::string error_of(net::io_context& ioc, net::awaitable<HttpResponse> aw) {
    try {
        run(ioc, std::move(aw));
    } catch (const std::exception& e) {
        return e.what();
    }
    return "";
}

std::string header_of(const MockRequest& r, const std::string& name) {
    for (const auto& [k, v] : r.headers)
        if (k.size() == name.size() && std::equal(k.begin(), k.end(), name.begin(), [](char a, char b) {
                return std::tolower(static_cast<unsigned char>(a)) == std::tolower(static_cast<unsigned char>(b));
            }))
            return v;
    return "";
}

}  // namespace

TEST_CASE("http client: post_json sends method, JSON content type, custom header, user agent and body", "[ops_client][integration]") {
    net::io_context ioc;
    MockRequest seen;
    MockHttpServer srv(ioc, [&](const MockRequest& r) {
        seen = r;
        return MockReply{200, "{\"ok\":true}"};
    });
    HttpClient client(ioc.get_executor(), HttpClientOptions{.user_agent = "evml-test/1"});
    const auto res = run(ioc, client.post_json(srv.url() + "hook", "{\"a\":1}", {{"X-API-Key", kSecret}}));

    CHECK(res.status == 200);
    CHECK(res.body == "{\"ok\":true}");
    CHECK(seen.method == "POST");
    CHECK(seen.target == "/hook");
    CHECK(seen.body == "{\"a\":1}");
    CHECK(header_of(seen, "Content-Type") == "application/json");
    CHECK(header_of(seen, "X-API-Key") == kSecret);
    CHECK(header_of(seen, "User-Agent") == "evml-test/1");
    CHECK(header_of(seen, "Connection") == "close");
}

TEST_CASE("http client: get sends no body and keeps the query string", "[ops_client][integration]") {
    net::io_context ioc;
    MockRequest seen;
    MockHttpServer srv(ioc, [&](const MockRequest& r) {
        seen = r;
        return MockReply{200, "OK"};
    });
    HttpClient client(ioc.get_executor());
    const auto res = run(ioc, client.get(srv.url() + "ping/abc?x=1", {{"X-Trace", "t1"}}));
    CHECK(res.status == 200);
    CHECK(res.body == "OK");
    CHECK(seen.method == "GET");
    CHECK(seen.target == "/ping/abc?x=1");
    CHECK(seen.body.empty());
    CHECK(header_of(seen, "X-Trace") == "t1");
}

TEST_CASE("http client: a non-2xx answer is returned to the caller, not thrown", "[ops_client][integration]") {
    net::io_context ioc;
    int n = 0;
    MockHttpServer srv(ioc, [&](const MockRequest&) { return ++n == 1 ? MockReply{500, "boom"} : MockReply{429, "slow down"}; });
    HttpClient client(ioc.get_executor());
    const auto a = run(ioc, client.post_json(srv.url(), "{}"));
    CHECK(a.status == 500);
    CHECK(a.body == "boom");
    const auto b = run(ioc, client.get(srv.url()));
    CHECK(b.status == 429);
}

TEST_CASE("http client: each request uses its own connection", "[ops_client][integration]") {
    net::io_context ioc;
    MockHttpServer srv(ioc, [](const MockRequest&) { return MockReply{200, "x"}; });
    HttpClient client(ioc.get_executor());
    run(ioc, client.get(srv.url()));
    run(ioc, client.get(srv.url()));
    CHECK(srv.connections() == 2);
}

TEST_CASE("http client: a server that never answers times out and the error hides the URL token", "[ops_client][integration]") {
    net::io_context ioc;
    MockHttpServer srv(ioc, [](const MockRequest&) {
        MockReply r;
        r.hang = true;
        return r;
    });
    HttpClient client(ioc.get_executor(), HttpClientOptions{.timeout = std::chrono::milliseconds(150)});
    const std::string err = error_of(ioc, client.post_json(srv.url() + kSecret, "{}", {{"X-API-Key", kSecret}}));
    CHECK_THAT(err, ContainsSubstring("read failed"));
    CHECK_THAT(err, ContainsSubstring("timeout"));
    CHECK(err.find(kSecret) == std::string::npos);
}

TEST_CASE("http client: connection refused is a TransportError without the URL token", "[ops_client][integration]") {
    net::io_context ioc;
    unsigned short port = 0;
    {
        tcp::acceptor a(ioc, tcp::endpoint(net::ip::make_address("127.0.0.1"), 0));
        port = a.local_endpoint().port();
    }  // closed: nothing listens on it
    HttpClient client(ioc.get_executor());
    const std::string err = error_of(ioc, client.get("http://127.0.0.1:" + std::to_string(port) + "/" + kSecret));
    CHECK_THAT(err, ContainsSubstring("connect failed"));
    CHECK(err.find(kSecret) == std::string::npos);
}

TEST_CASE("http client: https against a plain-HTTP port fails the TLS handshake without leaking the URL token", "[ops_client][integration]") {
    net::io_context ioc;
    MockHttpServer srv(ioc, [](const MockRequest&) { return MockReply{200, "x"}; });
    const std::string plain = srv.url();  // http://127.0.0.1:<port>/
    HttpClient client(ioc.get_executor(), HttpClientOptions{.timeout = std::chrono::milliseconds(2000)});
    const std::string err = error_of(ioc, client.get("https://" + plain.substr(7) + kSecret));
    CHECK_THAT(err, ContainsSubstring("TLS handshake failed"));
    CHECK(err.find(kSecret) == std::string::npos);
}

TEST_CASE("http client: bad URLs are TransportErrors with the secret redacted", "[ops_client]") {
    net::io_context ioc;
    HttpClient client(ioc.get_executor());
    for (const std::string& url : {std::string("ftp://example.com/") + kSecret, std::string("http://user:") + kSecret + "@example.com/",
                                  std::string("example.com/") + kSecret}) {
        const std::string err = error_of(ioc, client.get(url));
        CHECK_FALSE(err.empty());
        CHECK(err.find(kSecret) == std::string::npos);
    }
}

TEST_CASE("http client: a response above max_response_bytes is refused", "[ops_client][integration]") {
    net::io_context ioc;
    MockHttpServer srv(ioc, [](const MockRequest&) { return MockReply{200, std::string(5000, 'a')}; });
    HttpClient client(ioc.get_executor(), HttpClientOptions{.max_response_bytes = 1000});
    const std::string err = error_of(ioc, client.get(srv.url()));
    CHECK_THAT(err, ContainsSubstring("read failed"));
}

TEST_CASE("http client: headers that could break the request are rejected and never echoed", "[ops_client]") {
    net::io_context ioc;
    HttpClient client(ioc.get_executor());
    const auto fails = [&](HttpClient::Headers h) {
        try {
            run(ioc, client.get("http://127.0.0.1:9/", h));
        } catch (const std::invalid_argument& e) {
            CHECK(std::string(e.what()).find(kSecret) == std::string::npos);
            return true;
        } catch (...) {
        }
        return false;
    };
    CHECK(fails({{"X-API-Key", std::string(kSecret) + "\r\nX-Evil: 1"}}));
    CHECK(fails({{"Bad Name", "v"}}));
    CHECK(fails({{"", "v"}}));
    CHECK(fails({{"Host", "evil.example"}}));
    CHECK(fails({{"content-length", "5"}}));
    CHECK(fails({{"Connection", "keep-alive"}}));
}

TEST_CASE("http client: bad options are rejected", "[ops_client]") {
    net::io_context ioc;
    CHECK_THROWS_AS(HttpClient(ioc.get_executor(), HttpClientOptions{.timeout = std::chrono::milliseconds(0)}), std::invalid_argument);
    CHECK_THROWS_AS(HttpClient(ioc.get_executor(), HttpClientOptions{.max_response_bytes = 0}), std::invalid_argument);
}
