#include "rpc/blocking_http.hpp"

#include <optional>

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/ssl.hpp>

#include <openssl/ssl.h>

#include "core/env.hpp"
#include "rpc/url.hpp"

namespace evml::rpc {

namespace net = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
namespace ssl = net::ssl;
using tcp = net::ip::tcp;

namespace {

/// @brief Executes a single coroutine phase, wrapping Boost exceptions into TransportError with redacted URL.
template <class Awaitable>
net::awaitable<void> phase(const std::string& redacted, const char* name, Awaitable&& a) {
    try {
        co_await std::forward<Awaitable>(a);
    } catch (const boost::system::system_error& e) {
        // Redact credentials from error messages before bubbling up
        throw TransportError(std::string(name) + " failed for " + redacted + ": " + e.code().message());
    }
}

/// @brief Sends HTTP POST request and parses response body over an active stream (plain TCP or TLS).
template <class Stream>
net::awaitable<HttpResponse> exchange(Stream& stream, const ParsedUrl& u, const std::string& body, const std::string& user_agent,
                                      std::chrono::milliseconds timeout, const std::string& redacted) {
    // Construct HTTP 1.1 POST request
    http::request<http::string_body> req{http::verb::post, u.target, 11};
    req.set(http::field::host, u.host);
    req.set(http::field::user_agent, user_agent);
    req.set(http::field::content_type, "application/json");
    req.set(http::field::accept, "application/json");
    req.body() = body;
    // Compute Content-Length header
    req.prepare_payload();

    // Set deadline timer for request write phase
    beast::get_lowest_layer(stream).expires_after(timeout);
    co_await phase(redacted, "write", http::async_write(stream, req, net::use_awaitable));

    // Allocate buffer and parse incoming response
    beast::flat_buffer buf;
    http::response<http::string_body> res;
    // Set deadline timer for response read phase
    beast::get_lowest_layer(stream).expires_after(timeout);
    co_await phase(redacted, "read", http::async_read(stream, buf, res, net::use_awaitable));
    co_return HttpResponse{static_cast<int>(res.result_int()), std::move(res.body())};
}

/// @brief Asynchronous coroutine establishing DNS resolution, connection, optional TLS, and exchange.
net::awaitable<HttpResponse> post(const std::string& url, const std::string& body, std::chrono::milliseconds timeout,
                                  const std::string& user_agent) {
    // Redact secret query params or tokens before logging or throwing
    const std::string redacted = core::env::redact(url);
    const ParsedUrl u = parse_url(url, UrlKind::Http);
    auto executor = co_await net::this_coro::executor;

    // Resolve DNS hostname to IP endpoints
    tcp::resolver resolver(executor);
    std::optional<tcp::resolver::results_type> endpoints;
    {
        try {
            endpoints = co_await resolver.async_resolve(u.host, u.port, net::use_awaitable);
        } catch (const boost::system::system_error& e) {
            throw TransportError("resolve failed for " + redacted + ": " + e.code().message());
        }
    }

    // Branch 1: Plain unencrypted HTTP (used for local anvil/hardhat dev nodes)
    if (!u.tls) {
        beast::tcp_stream stream(executor);
        stream.expires_after(timeout);
        co_await phase(redacted, "connect", stream.async_connect(*endpoints, net::use_awaitable));
        auto r = co_await exchange(stream, u, body, user_agent, timeout, redacted);
        beast::error_code ec;
        stream.socket().shutdown(tcp::socket::shutdown_both, ec);
        co_return r;
    }

    // Branch 2: Encrypted HTTPS with strict certificate and SNI validation
    ssl::context ctx(ssl::context::tls_client);
    ctx.set_default_verify_paths();
    ctx.set_verify_mode(ssl::verify_peer);
    beast::ssl_stream<beast::tcp_stream> stream(executor, ctx);
    // Configure TLS Server Name Indication (SNI) extension to prevent SSL handshake errors on virtual hosts
    if (!SSL_set_tlsext_host_name(stream.native_handle(), u.host.c_str())) {
        throw TransportError("cannot set TLS SNI for " + redacted);
    }
    // Verify server certificate matches hostname
    stream.set_verify_callback(ssl::host_name_verification(u.host));

    // Connect underlying TCP socket
    beast::get_lowest_layer(stream).expires_after(timeout);
    co_await phase(redacted, "connect", beast::get_lowest_layer(stream).async_connect(*endpoints, net::use_awaitable));
    // Perform TLS client handshake
    beast::get_lowest_layer(stream).expires_after(timeout);
    co_await phase(redacted, "TLS handshake", stream.async_handshake(ssl::stream_base::client, net::use_awaitable));
    auto r = co_await exchange(stream, u, body, user_agent, timeout, redacted);

    // Best-effort close_notify; the response is already complete, so a failing shutdown is not an error.
    beast::get_lowest_layer(stream).expires_after(std::chrono::seconds(2));
    try {
        co_await stream.async_shutdown(net::use_awaitable);
    } catch (const boost::system::system_error&) {
    }
    co_return r;
}

}  // namespace

/// @brief Synchronously runs HTTP POST coroutine on a temporary io_context.
HttpResponse http_post_json(const std::string& url, const std::string& body, std::chrono::milliseconds timeout,
                            const std::string& user_agent) {
    net::io_context ioc;
    // Spawn coroutine and bind return value to std::future
    auto fut = net::co_spawn(ioc, post(url, body, timeout, user_agent), net::use_future);
    ioc.run();
    // Re-throw any captured TransportError from the coroutine
    return fut.get();
}

}  // namespace evml::rpc
