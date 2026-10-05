#include <catch2/catch_test_macros.hpp>

#include <boost/json.hpp>
#include <spdlog/sinks/ostream_sink.h>

#include <sstream>

#include "core/log.hpp"

namespace {
using namespace evml::core;

struct Capture {
    std::ostringstream os;
    std::shared_ptr<spdlog::logger> logger;
    explicit Capture(const std::string& level) {
        logger = log::make_logger(std::make_shared<spdlog::sinks::ostream_sink_mt>(os), level);
    }
    boost::json::object last() {
        auto s = os.str();
        REQUIRE(!s.empty());
        REQUIRE(s.back() == '\n');
        s.pop_back();
        return boost::json::parse(s).as_object();
    }
};
}  // namespace

TEST_CASE("log line is valid JSON with core and extra fields") {
    Capture c("info");
    log::emit(*c.logger, spdlog::level::info, "ws", "connected", {{"provider", "quicknode"}, {"attempt", "3"}});
    auto o = c.last();
    CHECK(o.at("level").as_string() == "info");
    CHECK(o.at("message").as_string() == "connected");
    CHECK(o.at("component").as_string() == "ws");
    CHECK(o.at("provider").as_string() == "quicknode");
    CHECK(o.at("attempt").as_string() == "3");
    const auto ts = std::string(o.at("ts").as_string());
    CHECK(ts.size() == 24);
    CHECK(ts.back() == 'Z');
}

TEST_CASE("log escaping round-trips") {
    Capture c("info");
    const std::string nasty = "q\"uote \\ back\nnl\ttab \x01 ctl \xE2\x9C\x93 utf8";
    log::emit(*c.logger, spdlog::level::warn, "t", nasty, {{"k", nasty}});
    auto o = c.last();
    CHECK(o.at("message").as_string() == nasty);
    CHECK(o.at("k").as_string() == nasty);
    CHECK(o.at("level").as_string() == "warn");
}

TEST_CASE("log level filtering") {
    {
        Capture c("info");
        log::emit(*c.logger, spdlog::level::debug, "t", "hidden");
        CHECK(c.os.str().empty());
    }
    {
        Capture c("debug");
        log::emit(*c.logger, spdlog::level::debug, "t", "shown");
        CHECK(c.last().at("message").as_string() == "shown");
    }
}

TEST_CASE("log rejects bad level") {
    std::ostringstream os;
    CHECK_THROWS(log::make_logger(std::make_shared<spdlog::sinks::ostream_sink_mt>(os), "loud"));
}
