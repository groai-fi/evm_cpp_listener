#pragma once

#include <chrono>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

namespace evml::rpc {

enum class FailKind {
    Retry,           // network/TLS/WS errors, timeouts, transient provider errors -> full-jitter backoff
    RateLimited,     // per-second throttling -> max(backoff, Retry-After)
    QuotaExhausted,  // monthly quota -> provider cooled down, immediate failover
    AuthFatal,       // bad key / forbidden -> provider disabled for a while
    ConfigFatal,     // chain id mismatch (set by the caller, never produced by classify())
    RangeTooLarge,   // not a connection failure: caller shrinks the getLogs range
};

const char* to_string(FailKind k);

// Everything known about a failed exchange. Unset fields are 0 / nullopt / empty.
struct ErrorInfo {
    int http_status{0};                                // 0 = no HTTP status (e.g. WebSocket frame)
    std::optional<std::int64_t> rpc_code;              // JSON-RPC error.code
    std::string message;                               // JSON-RPC error.message, or HTTP body excerpt
    std::optional<std::chrono::seconds> retry_after;   // parsed Retry-After header
};

struct Classified {
    FailKind kind{FailKind::Retry};
    std::optional<std::uint32_t> range_limit;          // RangeTooLarge: N parsed from "limited to a N range"
    std::optional<std::chrono::seconds> retry_after;   // RateLimited: provider hint
};

// Table-driven classification (ordered rules, message matching is case-insensitive). Unknown errors are
// Retry: they are surfaced via logs/metrics by the caller, never silently dropped.
Classified classify(const ErrorInfo& e);

// Retry-After: only the delta-seconds form is supported (HTTP-date yields nullopt).
std::optional<std::chrono::seconds> parse_retry_after(std::string_view header_value);

}  // namespace evml::rpc
