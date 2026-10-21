#pragma once

#include <string>

namespace evml::rpc {

/// @brief URL scheme category expected by the transport parser.
enum class UrlKind {
    Http,  // Supports http:// (plain) and https:// (TLS)
    Ws     // Supports ws:// (plain) and wss:// (TLS)
};

/// @brief Dissected URL components optimized for Boost.Beast HTTP/WS client handshakes.
struct ParsedUrl {
    bool tls{};           // True if https:// or wss://
    std::string host;     // Hostname or IP address (without port or userinfo)
    std::string port;     // Port string ("443", "80", or custom port)
    std::string target;   // Path and query string, always starts with '/'

    /// @brief Computes HTTP/WS Host header value (omits port if standard 80/443).
    /// @return Formatted host header (e.g., "eth.alchemy.com" or "127.0.0.1:8545").
    std::string host_header() const;
};

/// @brief Parses and validates an RPC endpoint URL.
/// @param url Full endpoint URL (e.g., "https://base-mainnet.g.alchemy.com/v2/...").
/// @param kind Expected scheme family (Http or Ws).
/// @return Dissected URL components.
/// @warning Throws TransportError with redacted URL on unsupported scheme, userinfo, or syntax errors.
ParsedUrl parse_url(const std::string& url, UrlKind kind);

}  // namespace evml::rpc
