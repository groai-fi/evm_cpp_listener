#pragma once

#include <memory>
#include <string>

#include <boost/asio/ssl.hpp>
#include <openssl/ssl.h>

namespace evml::rpc {

/// @brief Creates a TLS client context configured with the system trust store and peer verification.
inline std::shared_ptr<boost::asio::ssl::context> make_tls_client_context() {
    namespace ssl = boost::asio::ssl;
    auto ctx = std::make_shared<ssl::context>(ssl::context::tls_client);
    ctx->set_default_verify_paths();
    ctx->set_verify_mode(ssl::verify_peer);
    return ctx;
}

/// @brief Configures TLS Server Name Indication (SNI) and RFC 6125 hostname verification on a Beast/Asio SSL stream.
template <class SslStream>
inline bool configure_tls_sni_and_hostname(SslStream& stream, const std::string& host) {
    if (!SSL_set_tlsext_host_name(stream.native_handle(), host.c_str())) {
        return false;
    }
    stream.set_verify_callback(boost::asio::ssl::host_name_verification(host));
    return true;
}

}  // namespace evml::rpc

