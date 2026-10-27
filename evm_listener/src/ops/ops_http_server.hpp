#pragma once
// Ops HTTP server on the io thread (plan 60 §5.2, slice 5-B3): GET /healthz, /readyz, /metrics through callbacks that
// return status + body. Unknown path 404, other methods 405. Binds `[::]:port` dual-stack (IPv4 too) when the host is an
// IPv6 literal. The service has no public domain; no endpoint may put secrets in a body.
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>

#include <boost/asio/io_context.hpp>

namespace evml::ops {

struct OpsResponse {
    int status{200};
    std::string body;
    std::string content_type{"text/plain; charset=utf-8"};
};

class OpsHttpServer {
public:
    /// Runs on the io thread: must not block.
    using Handler = std::function<OpsResponse()>;
    struct Routes {
        Handler healthz;
        Handler readyz;
        Handler metrics;
    };

    /// @param listen "host:port" with an optional bracketed IPv6 host, e.g. "[::]:9464", "127.0.0.1:0" (port 0 = ephemeral).
    /// @param request_timeout per-connection deadline for reading one request and for an idle keep-alive (slow-client guard).
    /// @throws std::invalid_argument on a malformed `listen` or a null handler; boost::system::system_error if the bind fails.
    OpsHttpServer(boost::asio::io_context& ioc, const std::string& listen, Routes routes,
                  std::chrono::milliseconds request_timeout = std::chrono::seconds(10));
    ~OpsHttpServer();
    OpsHttpServer(const OpsHttpServer&) = delete;
    OpsHttpServer& operator=(const OpsHttpServer&) = delete;

    /// @brief The bound port (useful when `listen` used port 0).
    std::uint16_t port() const;
    /// @brief Stop accepting. Open sessions end at their next read. Idempotent; call on the io thread.
    void stop();

    struct State;  // implementation detail, public so the .cpp-local coroutines can name it

private:
    std::shared_ptr<State> st_;
};

}  // namespace evml::ops
