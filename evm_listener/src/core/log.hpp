#pragma once

#include <initializer_list>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include <spdlog/spdlog.h>

namespace evml::core::log {

using Field = std::pair<std::string_view, std::string_view>;

// Installs the process-wide logger: one JSON object per line on stdout with keys
// `ts` (ISO-8601 UTC, ms), `level`, `message`, `component`, then caller-supplied fields.
// Railway parses `level` and `message` as structured logs. Call once at startup.
void init(const std::string& level = "info");

// Same format into an arbitrary sink (tests). Returns a logger that is NOT installed globally.
std::shared_ptr<spdlog::logger> make_logger(std::shared_ptr<spdlog::sinks::sink> sink, const std::string& level = "info");

// Structured emit. `fields` values are JSON-escaped strings. Secrets must be redacted by the caller
// (use env::redact for every endpoint URL).
void emit(spdlog::logger& logger, spdlog::level::level_enum lvl, std::string_view component, std::string_view message,
          std::initializer_list<Field> fields = {});

void info(std::string_view component, std::string_view message, std::initializer_list<Field> fields = {});
void warn(std::string_view component, std::string_view message, std::initializer_list<Field> fields = {});
void error(std::string_view component, std::string_view message, std::initializer_list<Field> fields = {});
void debug(std::string_view component, std::string_view message, std::initializer_list<Field> fields = {});

// JSON string escaping (quotes, backslash, control chars). Exposed for tests.
std::string json_escape(std::string_view s);

}  // namespace evml::core::log
