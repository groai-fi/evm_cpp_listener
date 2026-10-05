#include "rpc/url.hpp"

#include "core/env.hpp"
#include "rpc/blocking_http.hpp"

namespace evml::rpc {

std::string ParsedUrl::host_header() const {
    const char* def = tls ? "443" : "80";
    return port == def ? host : host + ":" + port;
}

ParsedUrl parse_url(const std::string& url, UrlKind kind) {
    ParsedUrl p;
    std::string rest;
    const char* plain = kind == UrlKind::Http ? "http://" : "ws://";
    const char* secure = kind == UrlKind::Http ? "https://" : "wss://";
    if (url.rfind(secure, 0) == 0) {
        p.tls = true;
        rest = url.substr(std::string(secure).size());
    } else if (url.rfind(plain, 0) == 0) {
        rest = url.substr(std::string(plain).size());
    } else {
        throw TransportError("unsupported URL scheme: " + core::env::redact(url));
    }
    const auto slash = rest.find_first_of("/?");
    std::string authority = rest.substr(0, slash);
    p.target = slash == std::string::npos ? "/" : rest.substr(slash);
    if (p.target[0] == '?') p.target.insert(0, "/");
    if (authority.find('@') != std::string::npos) throw TransportError("URL userinfo is not supported: " + core::env::redact(url));
    const auto colon = authority.rfind(':');
    if (colon != std::string::npos && authority.find(']') == std::string::npos) {
        p.port = authority.substr(colon + 1);
        authority.resize(colon);
    } else {
        p.port = p.tls ? "443" : "80";
    }
    if (authority.empty() || p.port.empty()) throw TransportError("malformed URL: " + core::env::redact(url));
    p.host = authority;
    return p;
}

}  // namespace evml::rpc
