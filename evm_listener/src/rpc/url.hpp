#pragma once

#include <string>

namespace evml::rpc {

enum class UrlKind { Http, Ws };  // Http: http/https; Ws: ws/wss

struct ParsedUrl {
    bool tls{};
    std::string host;
    std::string port;
    std::string target;  // path + query, always starts with '/'

    // Value for the Host header / WS handshake: port is included only when it is not the scheme default.
    std::string host_header() const;
};

// Throws TransportError (message contains only the *redacted* URL) on a wrong scheme, userinfo or malformed URL.
ParsedUrl parse_url(const std::string& url, UrlKind kind);

}  // namespace evml::rpc
