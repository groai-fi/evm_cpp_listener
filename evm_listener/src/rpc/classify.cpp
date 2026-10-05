#include "rpc/classify.hpp"

#include <algorithm>
#include <cctype>

namespace evml::rpc {

namespace {

std::string lower(std::string_view s) {
    std::string out(s);
    std::transform(out.begin(), out.end(), out.begin(), [](unsigned char c) { return std::tolower(c); });
    return out;
}

bool has(const std::string& hay, std::string_view needle) { return hay.find(needle) != std::string::npos; }

// "eth_getLogs is limited to a 500 range" -> 500. nullopt when the phrase or digits are absent.
std::optional<std::uint32_t> parse_range_limit(const std::string& msg) {
    static constexpr std::string_view kPhrase = "limited to a ";
    const auto pos = msg.find(kPhrase);
    if (pos == std::string::npos) return std::nullopt;
    std::uint64_t n = 0;
    std::size_t i = pos + kPhrase.size();
    const std::size_t start = i;
    while (i < msg.size() && std::isdigit(static_cast<unsigned char>(msg[i])) && n < 0xFFFFFFFFULL) {
        n = n * 10 + static_cast<std::uint64_t>(msg[i] - '0');
        ++i;
    }
    if (i == start || n == 0 || n > 0xFFFFFFFFULL) return std::nullopt;
    return static_cast<std::uint32_t>(n);
}

}  // namespace

const char* to_string(FailKind k) {
    switch (k) {
        case FailKind::Retry: return "retry";
        case FailKind::RateLimited: return "rate_limited";
        case FailKind::QuotaExhausted: return "quota_exhausted";
        case FailKind::AuthFatal: return "auth_fatal";
        case FailKind::ConfigFatal: return "config_fatal";
        case FailKind::RangeTooLarge: return "range_too_large";
    }
    return "unknown";
}

Classified classify(const ErrorInfo& e) {
    const std::string m = lower(e.message);
    const auto code = e.rpc_code.value_or(0);

    // 1. Monthly quota (Alchemy "Monthly capacity limit exceeded"): must precede the generic rate-limit rule.
    if (has(m, "monthly") && (has(m, "capacity") || has(m, "limit") || has(m, "quota"))) {
        return {FailKind::QuotaExhausted, std::nullopt, std::nullopt};
    }

    // 2. getLogs range/result caps (public Base -32614, QuickNode -32615; matched by message too).
    const bool limited_phrase = has(m, "limited to a") && has(m, "range");
    if (code == -32614 || code == -32615 || e.http_status == 413 || limited_phrase ||
        has(m, "query returned more than") || has(m, "response size") || has(m, "range too large") ||
        has(m, "range is too large") || (has(m, "block range") && (has(m, "too large") || has(m, "exceed")))) {
        return {FailKind::RangeTooLarge, parse_range_limit(m), std::nullopt};
    }

    // 3. Credentials.
    if (e.http_status == 401 || e.http_status == 403 || has(m, "invalid project id") || has(m, "invalid api key") ||
        has(m, "unauthorized")) {
        return {FailKind::AuthFatal, std::nullopt, std::nullopt};
    }

    // 4. Per-second throttling.
    if (e.http_status == 429 || code == -32005 || has(m, "rate limit") || has(m, "too many requests") ||
        has(m, "exceeded") || has(m, "capacity")) {
        return {FailKind::RateLimited, std::nullopt, e.retry_after};
    }

    // 5. Everything else (incl. Infura -32603 "service temporarily unavailable") is retried with backoff.
    return {FailKind::Retry, std::nullopt, std::nullopt};
}

std::optional<std::chrono::seconds> parse_retry_after(std::string_view v) {
    while (!v.empty() && std::isspace(static_cast<unsigned char>(v.front()))) v.remove_prefix(1);
    while (!v.empty() && std::isspace(static_cast<unsigned char>(v.back()))) v.remove_suffix(1);
    if (v.empty() || v.size() > 9) return std::nullopt;
    std::int64_t n = 0;
    for (const char c : v) {
        if (!std::isdigit(static_cast<unsigned char>(c))) return std::nullopt;
        n = n * 10 + (c - '0');
    }
    return std::chrono::seconds(n);
}

}  // namespace evml::rpc
