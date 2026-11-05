#pragma once

#include <openssl/ssl.h>

#include <boost/asio/ssl.hpp>
#include <memory>
#include <string>
#include <string_view>

namespace evml::rpc {

/// @brief Creates a TLS client context configured with the system trust store and peer verification.
[[nodiscard]] inline std::shared_ptr<boost::asio::ssl::context> make_tls_client_context() {
    namespace ssl = boost::asio::ssl;
    auto ctx = std::make_shared<ssl::context>(ssl::context::tls_client);
    ctx->set_default_verify_paths();
    ctx->set_verify_mode(ssl::verify_peer);
    return ctx;
}

/// @brief Configures TLS Server Name Indication (SNI) and RFC 6125 hostname verification on a Beast/Asio SSL stream.
template <class SslStream>
[[nodiscard]] inline bool configure_tls_sni_and_hostname(SslStream& stream, std::string_view host) {
    const std::string host_str(host);
    if (!SSL_set_tlsext_host_name(stream.native_handle(), host_str.c_str())) {
        return false;
    }
    stream.set_verify_callback(boost::asio::ssl::host_name_verification(host_str));
    return true;
}

}  // namespace evml::rpc
