#pragma once

#include <chrono>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

namespace evml::rpc {

/// @brief Failure classification category for RPC error dispatching and recovery.
enum class FailKind {
    Retry,           // network/TLS/WS errors, timeouts, transient provider errors -> full-jitter backoff
    RateLimited,     // per-second throttling -> max(backoff, Retry-After)
    QuotaExhausted,  // monthly quota -> provider cooled down, immediate failover
    AuthFatal,       // bad key / forbidden -> provider disabled for a while
    ConfigFatal,     // chain id mismatch (set by the caller, never produced by classify())
    RangeTooLarge,   // not a connection failure: caller shrinks the getLogs range
};

/// @brief Converts failure classification category to human-readable string.
const char* to_string(FailKind k);

/// @brief Unified error envelope capturing HTTP status, JSON-RPC error codes, and server response hints.
/// All fields are optional or defaulted; unset values indicate absence in the raw exchange.
struct ErrorInfo {
    int http_status{0};                                // 0 = no HTTP status (e.g. WebSocket frame)
    std::optional<std::int64_t> rpc_code;              // JSON-RPC error.code (-32614, -32005, etc.)
    std::string message;                               // JSON-RPC error.message, or HTTP body excerpt
    std::optional<std::chrono::seconds> retry_after;   // parsed Retry-After header duration
};

/// @brief Result of error classification informing recovery, paging, and failover behavior.
struct Classified {
    FailKind kind{FailKind::Retry};                    // Actionable failure category
    std::optional<std::uint32_t> range_limit;          // RangeTooLarge: N parsed from "limited to a N range"
    std::optional<std::chrono::seconds> retry_after;   // RateLimited: provider backoff hint
};

/// @brief Classifies an RPC failure using prioritized heuristics across codes, status, and error text.
/// Classification precedence:
/// 1. Monthly quota exhaustion (cooldown/failover)
/// 2. Range too large (trigger AIMD page shrinking)
/// 3. Authentication fatal (bad credentials)
/// 4. Per-second rate limiting (backoff with Retry-After)
/// 5. Transient retry (network glitches, timeouts, 5xx)
/// @param e Error envelope from HTTP response or JSON-RPC error frame.
/// @return Classified failure verdict and actionable metadata.
Classified classify(const ErrorInfo& e);

/// @brief Parses an HTTP Retry-After header value into delta seconds.
/// @param header_value Raw header string (e.g., "120").
/// @return Parsed seconds if numeric; std::nullopt for HTTP-date format or malformed input.
std::optional<std::chrono::seconds> parse_retry_after(std::string_view header_value);

}  // namespace evml::rpc
