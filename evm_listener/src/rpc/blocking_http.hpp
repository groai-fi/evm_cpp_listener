#pragma once

#include <chrono>
#include <stdexcept>
#include <string>

namespace evml::rpc {

// Network/TLS/HTTP-level failure. Messages only ever contain the *redacted* URL.
struct TransportError : std::runtime_error {
    using std::runtime_error::runtime_error;
};

struct HttpResponse {
    int status{};
    std::string body;
};

// One-shot blocking HTTPS (or plain HTTP, for local test nodes) POST of a JSON body, with a deadline per phase
// (resolve/connect, TLS handshake, write, read). Certificates and host name are verified against the system
// trust store. Intended for CLI tooling (--check-config, --find-deploy-block); the long-lived runtime uses the
// async transport. Throws TransportError.
HttpResponse http_post_json(const std::string& url, const std::string& body, std::chrono::milliseconds timeout,
                            const std::string& user_agent);

}  // namespace evml::rpc
