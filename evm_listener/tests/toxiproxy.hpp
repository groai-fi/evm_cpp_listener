// Test helper: a toxiproxy (https://github.com/Shopify/toxiproxy) client for the Redis integration tests.
//
// TOXIPROXY_URL (e.g. http://toxiproxy:8474) and REDIS_URL come from the process environment only (the dev container
// sets both). A missing or unreachable toxiproxy FAILS the test; it never skips it.
//
// A ToxiRedis is one proxy in front of the compose Redis, listening on a random port chosen by toxiproxy. Connect the
// sink to `url()`; `disable()` cuts every connection and refuses new ones (a network outage), `enable()` restores it,
// `add_toxic()` shapes the traffic (latency, reset_peer).
#pragma once

#include <catch2/catch_test_macros.hpp>

#include <cstdlib>
#include <iostream>
#include <random>
#include <stdexcept>
#include <string>

#include <boost/asio/connect.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/json.hpp>

namespace evml::testing {

inline std::string require_env(const char* name) {
    const char* v = std::getenv(name);
    if (!v || !*v) FAIL(std::string(name) + " is not set: the sink integration tests must run in the dev container (never skipped)");
    return v;
}

class ToxiRedis {
public:
    ToxiRedis() {
        parse_api(require_env("TOXIPROXY_URL"));
        upstream_ = upstream_of(require_env("REDIS_URL"));

        std::random_device rd;
        name_ = "evml_" + std::to_string(rd()) + std::to_string(rd());
        boost::json::object body{{"name", name_}, {"listen", "0.0.0.0:0"}, {"upstream", upstream_}, {"enabled", true}};
        const auto resp = boost::json::parse(call(boost::beast::http::verb::post, "/proxies", boost::json::serialize(body)));
        const std::string listen = std::string(resp.as_object().at("listen").as_string());
        const auto colon = listen.rfind(':');
        if (colon == std::string::npos) throw std::runtime_error("toxiproxy: unexpected listen address '" + listen + "'");
        port_ = listen.substr(colon + 1);
        if (port_ == "0") throw std::runtime_error("toxiproxy did not assign a port: '" + listen + "'");
    }

    ~ToxiRedis() {
        try {
            call(boost::beast::http::verb::delete_, "/proxies/" + name_, "");
        } catch (const std::exception& e) {
            std::cerr << "toxiproxy cleanup of proxy " << name_ << " failed: " << e.what() << "\n";
        }
    }
    ToxiRedis(const ToxiRedis&) = delete;
    ToxiRedis& operator=(const ToxiRedis&) = delete;

    // The Redis URL a client under test must use to go through the proxy.
    std::string url() const { return "redis://" + api_host_ + ":" + port_ + "/0"; }

    void disable() { set_enabled(false); }
    void enable() { set_enabled(true); }

    // `type`: "latency" (attrs {"latency":20}), "reset_peer" (attrs {"timeout":0}), ...
    void add_toxic(const std::string& toxic_name, const std::string& type, const boost::json::object& attributes,
                   const std::string& stream = "downstream") {
        boost::json::object body{{"name", toxic_name}, {"type", type}, {"stream", stream}, {"toxicity", 1.0}, {"attributes", attributes}};
        call(boost::beast::http::verb::post, "/proxies/" + name_ + "/toxics", boost::json::serialize(body));
    }
    void remove_toxic(const std::string& toxic_name) {
        call(boost::beast::http::verb::delete_, "/proxies/" + name_ + "/toxics/" + toxic_name, "");
    }

private:
    void parse_api(const std::string& url) {
        const std::string scheme = "http://";
        if (url.rfind(scheme, 0) != 0) throw std::runtime_error("TOXIPROXY_URL must start with http://: " + url);
        std::string hp = url.substr(scheme.size());
        if (const auto slash = hp.find('/'); slash != std::string::npos) hp.resize(slash);
        const auto colon = hp.rfind(':');
        if (colon == std::string::npos) throw std::runtime_error("TOXIPROXY_URL needs a port: " + url);
        api_host_ = hp.substr(0, colon);
        api_port_ = hp.substr(colon + 1);
    }

    // "redis://host:port/db" -> "host:port" (the address toxiproxy forwards to, resolved inside its own container).
    static std::string upstream_of(const std::string& redis_url) {
        const std::string scheme = "redis://";
        if (redis_url.rfind(scheme, 0) != 0) throw std::runtime_error("REDIS_URL must start with redis://: " + redis_url);
        std::string hp = redis_url.substr(scheme.size());
        if (const auto slash = hp.find('/'); slash != std::string::npos) hp.resize(slash);
        if (hp.find('@') != std::string::npos) throw std::runtime_error("REDIS_URL with credentials is not supported by the toxiproxy helper");
        if (hp.find(':') == std::string::npos) hp += ":6379";
        return hp;
    }

    void set_enabled(bool on) {
        boost::json::object body{{"enabled", on}};
        call(boost::beast::http::verb::post, "/proxies/" + name_, boost::json::serialize(body));
    }

    // One blocking HTTP request, new connection each time. Throws on a transport failure or a status >= 300.
    std::string call(boost::beast::http::verb verb, const std::string& target, const std::string& body) {
        namespace http = boost::beast::http;
        boost::asio::io_context io;
        boost::asio::ip::tcp::resolver resolver(io);
        boost::beast::tcp_stream stream(io);
        stream.expires_after(std::chrono::seconds(5));
        stream.connect(resolver.resolve(api_host_, api_port_));

        http::request<http::string_body> req{verb, target, 11};
        req.set(http::field::host, api_host_);
        req.set(http::field::content_type, "application/json");
        req.body() = body;
        req.prepare_payload();
        http::write(stream, req);

        boost::beast::flat_buffer buf;
        http::response<http::string_body> res;
        http::read(stream, buf, res);
        boost::beast::error_code ec;
        stream.socket().shutdown(boost::asio::ip::tcp::socket::shutdown_both, ec);

        if (res.result_int() >= 300)
            throw std::runtime_error("toxiproxy " + std::string(http::to_string(verb)) + " " + target + " -> " +
                                     std::to_string(res.result_int()) + ": " + res.body());
        return res.body();
    }

    std::string api_host_, api_port_, upstream_, name_, port_;
};

}  // namespace evml::testing
