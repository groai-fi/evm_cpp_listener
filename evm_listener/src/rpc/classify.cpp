#include "rpc/classify.hpp"

#include <algorithm>
#include <cctype>

namespace evml::rpc {

namespace {

constexpr char ascii_lower(char c) noexcept {
    return (c >= 'A' && c <= 'Z') ? static_cast<char>(c - 'A' + 'a') : c;
}

/// @brief Case-insensitive substring search (`needle` must be lowercase ASCII) without heap allocation.
std::size_t find_icase(std::string_view hay, std::string_view needle) noexcept {
    if (needle.empty()) return 0;
    if (hay.size() < needle.size()) return std::string_view::npos;
    const std::size_t limit = hay.size() - needle.size();
    for (std::size_t i = 0; i <= limit; ++i) {
        bool match = true;
        for (std::size_t j = 0; j < needle.size(); ++j) {
            if (ascii_lower(hay[i + j]) != needle[j]) {
                match = false;
                break;
            }
        }
        if (match) return i;
    }
    return std::string_view::npos;
}

/// @brief Case-insensitive substring containment check.
bool has(std::string_view hay, std::string_view needle) noexcept {
    return find_icase(hay, needle) != std::string_view::npos;
}

/// @brief Parses block range limit numbers from provider error messages (e.g., "eth_getLogs is limited to a 500 range").
/// @param msg Error message text.
/// @return Extracted block limit, or nullopt if pattern not matched.
std::optional<std::uint32_t> parse_range_limit(std::string_view msg) noexcept {
    static constexpr std::string_view kPhrase = "limited to a ";
    // Search for anchor phrase in message
    const auto pos = find_icase(msg, kPhrase);
    if (pos == std::string_view::npos) return std::nullopt;
    std::uint64_t n = 0;
    std::size_t i = pos + kPhrase.size();
    const std::size_t start = i;
    // Extract contiguous decimal digits following the anchor phrase
    while (i < msg.size() && msg[i] >= '0' && msg[i] <= '9' && n < 0xFFFFFFFFULL) {
        n = n * 10 + static_cast<std::uint64_t>(msg[i] - '0');
        ++i;
    }
    // Validate that digits were consumed and value fits within 32-bit uint
    if (i == start || n == 0 || n > 0xFFFFFFFFULL) return std::nullopt;
    return static_cast<std::uint32_t>(n);
}

}  // namespace

/// @brief Classifies an RPC failure using ordered pattern matching across status codes and error messages.
Classified classify(const ErrorInfo& e) noexcept {
    const std::string_view m = e.message;
    const auto code = e.rpc_code.value_or(0);

    // Rule 1: Monthly quota exhaustion (Alchemy "Monthly capacity limit exceeded").
    // Must precede generic rate-limit rules because monthly caps require multi-hour cooldowns.
    if (has(m, "monthly") && (has(m, "capacity") || has(m, "limit") || has(m, "quota"))) {
        return {FailKind::QuotaExhausted, std::nullopt, std::nullopt};
    }

    // Rule 2: Query range / result size limits (Base -32614, QuickNode -32615, HTTP 413 Payload Too Large).
    // Directs caller to shrink getLogs window via AIMD rather than retrying the same payload.
    const bool limited_phrase = has(m, "limited to a") && has(m, "range");
    if (code == -32614 || code == -32615 || e.http_status == 413 || limited_phrase ||
        has(m, "query returned more than") || has(m, "response size") || has(m, "range too large") ||
        has(m, "range is too large") || (has(m, "block range") && (has(m, "too large") || has(m, "exceed")))) {
        return {FailKind::RangeTooLarge, parse_range_limit(m), std::nullopt};
    }

    // Rule 3: Authentication failures (HTTP 401/403, bad project ID, invalid key).
    // Marks provider disabled to prevent wasting budget on dead credentials.
    if (e.http_status == 401 || e.http_status == 403 || has(m, "invalid project id") || has(m, "invalid api key") ||
        has(m, "unauthorized")) {
        return {FailKind::AuthFatal, std::nullopt, std::nullopt};
    }

    // Rule 4: Transient per-second rate limiting (HTTP 429, Infura/Alchemy -32005).
    // Honors Retry-After header if provided by server.
    if (e.http_status == 429 || code == -32005 || has(m, "rate limit") || has(m, "too many requests") ||
        has(m, "exceeded") || has(m, "capacity")) {
        return {FailKind::RateLimited, std::nullopt, e.retry_after};
    }

    // Rule 5: Default catch-all for network errors, timeouts, and transient 5xx server issues.
    return {FailKind::Retry, std::nullopt, std::nullopt};
}

/// @brief Parses delta-seconds representation of HTTP Retry-After header.
std::optional<std::chrono::seconds> parse_retry_after(std::string_view v) noexcept {
    // Strip leading whitespace
    while (!v.empty() && std::isspace(static_cast<unsigned char>(v.front()))) v.remove_prefix(1);
    // Strip trailing whitespace
    while (!v.empty() && std::isspace(static_cast<unsigned char>(v.back()))) v.remove_suffix(1);
    // Reject empty strings or unreasonably long values (> 9 digits ~ 31 years)
    if (v.empty() || v.size() > 9) return std::nullopt;
    std::int64_t n = 0;
    // Accumulate digits; abort if non-digit character is encountered
    for (const char c : v) {
        if (!std::isdigit(static_cast<unsigned char>(c))) return std::nullopt;
        n = n * 10 + (c - '0');
    }
    return std::chrono::seconds(n);
}

}  // namespace evml::rpc
