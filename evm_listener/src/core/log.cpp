#include "core/log.hpp"

#include <chrono>
#include <cstdio>
#include <ctime>

#include <spdlog/sinks/stdout_sinks.h>

namespace evml::core::log {

namespace {

std::string iso_utc_ms() {
    const auto now = std::chrono::system_clock::now();
    const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000;
    const std::time_t t = std::chrono::system_clock::to_time_t(now);
    std::tm tm{};
    gmtime_r(&t, &tm);
    char buf[96];
    std::snprintf(buf, sizeof(buf), "%04d-%02d-%02dT%02d:%02d:%02d.%03dZ", tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
                  tm.tm_hour, tm.tm_min, tm.tm_sec, static_cast<int>(ms.count()));
    return buf;
}

const char* level_name(spdlog::level::level_enum l) {
    switch (l) {
        case spdlog::level::trace: return "trace";
        case spdlog::level::debug: return "debug";
        case spdlog::level::info: return "info";
        case spdlog::level::warn: return "warn";
        case spdlog::level::err: return "error";
        case spdlog::level::critical: return "critical";
        default: return "info";
    }
}

spdlog::level::level_enum parse_level(const std::string& s) {
    const auto l = spdlog::level::from_str(s);
    // from_str maps unknown strings to `off`; that would silence all logs, so reject it explicitly.
    if (l == spdlog::level::off && s != "off") throw std::invalid_argument("unknown log level: " + s);
    return l;
}

std::shared_ptr<spdlog::logger> g_logger;

spdlog::logger& global() {
    if (!g_logger) throw std::logic_error("log::init() must be called before logging");
    return *g_logger;
}

}  // namespace

std::string json_escape(std::string_view s) {
    std::string out;
    out.reserve(s.size() + 2);
    for (const unsigned char c : s) {
        switch (c) {
            case '"': out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\n': out += "\\n"; break;
            case '\r': out += "\\r"; break;
            case '\t': out += "\\t"; break;
            default:
                if (c < 0x20) {
                    char buf[8];
                    std::snprintf(buf, sizeof(buf), "\\u%04x", c);
                    out += buf;
                } else {
                    out += static_cast<char>(c);
                }
        }
    }
    return out;
}

std::shared_ptr<spdlog::logger> make_logger(std::shared_ptr<spdlog::sinks::sink> sink, const std::string& level) {
    auto logger = std::make_shared<spdlog::logger>("evml", std::move(sink));
    logger->set_pattern("%v");  // the message is already a complete JSON line
    logger->set_level(parse_level(level));
    return logger;
}

void init(const std::string& level) {
    g_logger = make_logger(std::make_shared<spdlog::sinks::stdout_sink_mt>(), level);
    g_logger->flush_on(spdlog::level::warn);
}

void emit(spdlog::logger& logger, spdlog::level::level_enum lvl, std::string_view component, std::string_view message,
          std::initializer_list<Field> fields) {
    if (!logger.should_log(lvl)) return;
    std::string line = "{\"ts\":\"" + iso_utc_ms() + "\",\"level\":\"" + level_name(lvl) + "\",\"message\":\"" +
                       json_escape(message) + "\",\"component\":\"" + json_escape(component) + "\"";
    for (const auto& [k, v] : fields) {
        line += ",\"" + json_escape(k) + "\":\"" + json_escape(v) + "\"";
    }
    line += '}';
    logger.log(lvl, line);
}

void info(std::string_view c, std::string_view m, std::initializer_list<Field> f) { emit(global(), spdlog::level::info, c, m, f); }
void warn(std::string_view c, std::string_view m, std::initializer_list<Field> f) { emit(global(), spdlog::level::warn, c, m, f); }
void error(std::string_view c, std::string_view m, std::initializer_list<Field> f) { emit(global(), spdlog::level::err, c, m, f); }
void debug(std::string_view c, std::string_view m, std::initializer_list<Field> f) { emit(global(), spdlog::level::debug, c, m, f); }

}  // namespace evml::core::log
