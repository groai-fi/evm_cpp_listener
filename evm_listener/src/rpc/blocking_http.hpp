#pragma once

#include <chrono>
#include <stdexcept>
#include <string>

namespace evml::rpc {

/// @brief Network-, TLS-, or protocol-level failure with guaranteed token redaction in error messages.
struct TransportError : std::runtime_error {
    using std::runtime_error::runtime_error;
};

/// @brief HTTP response payload carrying numeric status code and response body text.
struct HttpResponse {
    int status{};          // HTTP status code (e.g. 200, 429, 500)
    std::string body;      // Raw response body string
};

/// @brief One-shot blocking HTTP/HTTPS POST for JSON-RPC payloads (used by CLI tools and startup checks).
/// Executes coroutine pipeline on a private io_context with phase deadlines (DNS, connect, TLS, write, read).
/// Enforces TLS certificate verification and SNI against the system trust store.
/// @param url Target endpoint URL (may contain secret tokens; will be redacted on error).
/// @param body Serialized JSON-RPC request body.
/// @param timeout Per-phase timeout deadline.
/// @param user_agent User-Agent string sent in request headers.
/// @return HttpResponse containing status code and response body.
/// @warning Throws TransportError on connection failure, timeout, or TLS verification failure.
HttpResponse http_post_json(const std::string& url, const std::string& body, std::chrono::milliseconds timeout,
                            const std::string& user_agent);

}  // namespace evml::rpc
