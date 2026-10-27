#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <boost/asio.hpp>

namespace evml::test {

struct MockRequest {
    std::string target;
    std::string user_agent;
    std::string accept_encoding;
    std::string body;
    std::string method;                                       // "GET", "POST", ...
    std::vector<std::pair<std::string, std::string>> headers;  // every request header, in arrival order
};

struct MockReply {
    int status{200};
    std::string body;
    std::vector<std::pair<std::string, std::string>> headers{};
    std::chrono::milliseconds delay{0};
    bool hang{false};              // read the request, never answer (half-open / stuck server)
    bool connection_close{false};  // answer with `Connection: close` and close the socket
    bool close_silently{false};    // answer normally (keep-alive), then drop the socket (idle-kill by a LB)
};

// Scripted in-process HTTP/1.1 server on 127.0.0.1:<ephemeral>. Lives on the caller's io_context (single thread):
// the handler runs on that thread, so it may touch test locals without synchronisation.
class MockHttpServer {
public:
    using Handler = std::function<MockReply(const MockRequest&)>;

    MockHttpServer(boost::asio::io_context& ioc, Handler handler);

    std::string url() const;  // http://127.0.0.1:<port>/
    int connections() const;  // accepted TCP connections
    int requests() const;     // fully received requests

    struct State;  // implementation detail, public only so the .cpp-local coroutines can name it

private:
    std::shared_ptr<State> st_;
};

}  // namespace evml::test
