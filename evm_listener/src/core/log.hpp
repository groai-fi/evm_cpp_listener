#pragma once

#include <initializer_list>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include <spdlog/spdlog.h>

namespace evml::core::log {

/// @brief Key-value pair of string views representing a structured JSON log attribute.
using Field = std::pair<std::string_view, std::string_view>;

/// @brief Installs the process-wide structured JSON logger outputting to stdout.
/// Emits single-line JSON records containing `ts` (ISO-8601 UTC with ms), `level`, `message`, and `component`.
/// Flushes immediately on warn or error severity.
/// @param level Minimum log severity ("trace", "debug", "info", "warn", "error").
/// @warning Call exactly once at application startup.
void init(const std::string& level = "info");

/// @brief Factory creating an isolated logger writing to an arbitrary spdlog sink (used in unit tests).
/// @return Shared pointer to configured logger (not registered globally).
std::shared_ptr<spdlog::logger> make_logger(std::shared_ptr<spdlog::sinks::sink> sink, const std::string& level = "info");

/// @brief Emits a structured JSON log entry to the specified logger.
/// @param logger Destination spdlog logger.
/// @param lvl Severity level enum.
/// @param component Subsystem name (e.g. "pipeline", "sink", "rpc").
/// @param message High-level description of event.
/// @param fields Additional key-value pairs formatted as JSON properties.
/// @warning Caller must redact sensitive secrets or tokens before passing into fields.
void emit(spdlog::logger& logger, spdlog::level::level_enum lvl, std::string_view component, std::string_view message,
          std::initializer_list<Field> fields = {});

/// @brief Emits an INFO level structured log line to the global logger.
void info(std::string_view component, std::string_view message, std::initializer_list<Field> fields = {});
/// @brief Emits a WARN level structured log line to the global logger.
void warn(std::string_view component, std::string_view message, std::initializer_list<Field> fields = {});
/// @brief Emits an ERROR level structured log line to the global logger.
void error(std::string_view component, std::string_view message, std::initializer_list<Field> fields = {});
/// @brief Emits a DEBUG level structured log line to the global logger.
void debug(std::string_view component, std::string_view message, std::initializer_list<Field> fields = {});

/// @brief Escapes special characters (quotes, backslashes, control characters) for JSON string literals.
std::string json_escape(std::string_view s);

}  // namespace evml::core::log
