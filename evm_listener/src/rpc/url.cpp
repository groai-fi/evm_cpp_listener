#include "rpc/url.hpp"

#include "core/env.hpp"
#include "rpc/blocking_http.hpp"

namespace evml::rpc {

/// @brief Formats Host header, suppressing standard 80/443 default ports per RFC 7230 §5.4.
std::string ParsedUrl::host_header() const {
    // Select standard default port based on TLS presence
    const char* def = tls ? "443" : "80";
    // Append port only when non-default to preserve canonical host formatting
    return port == def ? host : host + ":" + port;
}

/// @brief Non-throwing URL parser returning Result<ParsedUrl>, redacting secrets on failure.
core::Result<ParsedUrl> try_parse_url(std::string_view url, UrlKind kind) {
    ParsedUrl p;
    std::string rest;
    // Define expected scheme prefixes
    const std::string_view plain = kind == UrlKind::Http ? "http://" : "ws://";
    const std::string_view secure = kind == UrlKind::Http ? "https://" : "wss://";
    // Check for secure TLS scheme prefix first
    if (url.substr(0, secure.size()) == secure) {
        p.tls = true;
        rest = std::string(url.substr(secure.size()));
    } else if (url.substr(0, plain.size()) == plain) {
        // Plain unencrypted scheme prefix
        rest = std::string(url.substr(plain.size()));
    } else {
        // Reject invalid or mismatched scheme family with redacted URL
        return core::Error{"unsupported URL scheme: " + core::env::redact(url)};
    }
    // Locate boundary between authority and path/query
    const auto slash = rest.find_first_of("/?");
    std::string authority = rest.substr(0, slash);
    // Ensure target always has at least a root slash
    p.target = slash == std::string::npos ? "/" : rest.substr(slash);
    // Fix bare query strings to have leading slash
    if (p.target[0] == '?') p.target.insert(0, "/");
    // Reject userinfo (@) to prevent credential leakage or ambiguous authority parsing
    if (authority.find('@') != std::string::npos) {
        return core::Error{"URL userinfo is not supported: " + core::env::redact(url)};
    }
    // Find optional port separator (ignoring IPv6 literal brackets)
    const auto colon = authority.rfind(':');
    if (colon != std::string::npos && authority.find(']') == std::string::npos) {
        // Extract explicit port number
        p.port = authority.substr(colon + 1);
        authority.resize(colon);
    } else {
        // Apply default port for protocol
        p.port = p.tls ? "443" : "80";
    }
    // Verify non-empty host and port fields
    if (authority.empty() || p.port.empty()) {
        return core::Error{"malformed URL: " + core::env::redact(url)};
    }
    p.host = authority;
    return p;
}

/// @brief Parses and validates an RPC HTTP or WS URL, redacting secrets on failure.
ParsedUrl parse_url(std::string_view url, UrlKind kind) {
    auto r = try_parse_url(url, kind);
    if (!r) throw TransportError(r.error().what());
    return std::move(*r);
}

}  // namespace evml::rpc
