#include "core/log.hpp"

#include <chrono>
#include <cstdio>
#include <ctime>

#include <spdlog/sinks/stdout_sinks.h>

namespace evml::core::log {

namespace {

/// @brief Generates high-resolution UTC ISO-8601 timestamp string with millisecond precision (e.g., 2026-10-21T18:30:00.123Z).
std::string iso_utc_ms() {
    const auto now = std::chrono::system_clock::now();
    const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000;
    const std::time_t t = std::chrono::system_clock::to_time_t(now);
    std::tm tm{};
    // Thread-safe UTC calendar decomposition
    gmtime_r(&t, &tm);
    char buf[96];
    std::snprintf(buf, sizeof(buf), "%04d-%02d-%02dT%02d:%02d:%02d.%03dZ", tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
                  tm.tm_hour, tm.tm_min, tm.tm_sec, static_cast<int>(ms.count()));
    return buf;
}

/// @brief Maps spdlog severity level enum to lowercase JSON string.
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

/// @brief Parses user-supplied log level string, failing fast on unknown strings.
spdlog::level::level_enum parse_level(const std::string& s) {
    const auto l = spdlog::level::from_str(s);
    // spdlog from_str maps unrecognized strings to 'off', which would silence all logs; reject explicitly
    if (l == spdlog::level::off && s != "off") throw std::invalid_argument("unknown log level: " + s);
    return l;
}

std::shared_ptr<spdlog::logger> g_logger;

/// @brief Retrieves process-wide logger instance; verifies initialization.
spdlog::logger& global() {
    if (!g_logger) throw std::logic_error("log::init() must be called before logging");
    return *g_logger;
}

}  // namespace

/// @brief Encodes control characters and quotes into valid JSON string literal characters.
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
                // Escape ASCII control codes < 0x20 into unicode hex escape
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

/// @brief Configures a standalone logger with raw pattern matching single-line JSON format.
std::shared_ptr<spdlog::logger> make_logger(std::shared_ptr<spdlog::sinks::sink> sink, const std::string& level) {
    auto logger = std::make_shared<spdlog::logger>("evml", std::move(sink));
    // Use %v (raw payload) pattern because emit() constructs self-contained JSON records
    logger->set_pattern("%v");
    logger->set_level(parse_level(level));
    return logger;
}

/// @brief Installs the process-wide stdout logger.
void init(const std::string& level) {
    g_logger = make_logger(std::make_shared<spdlog::sinks::stdout_sink_mt>(), level);
    // Ensure warnings and errors are immediately visible in stdout streams without buffering lag
    g_logger->flush_on(spdlog::level::warn);
}

/// @brief Emits a structured JSON record with standard header fields and caller attributes.
void emit(spdlog::logger& logger, spdlog::level::level_enum lvl, std::string_view component, std::string_view message,
          std::initializer_list<Field> fields) {
    // Early exit if logger configured level suppresses this severity
    if (!logger.should_log(lvl)) return;
    // Build root JSON object properties: ts, level, message, component
    std::string line = "{\"ts\":\"" + iso_utc_ms() + "\",\"level\":\"" + level_name(lvl) + "\",\"message\":\"" +
                       json_escape(message) + "\",\"component\":\"" + json_escape(component) + "\"";
    // Append user-supplied key-value pairs
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
