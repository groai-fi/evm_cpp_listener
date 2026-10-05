#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <boost/asio.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/json.hpp>

namespace evml::test {

// Server side of one accepted WebSocket connection, handed to the test's script.
class MockWsConn {
public:
    struct Req {
        std::uint64_t id{};
        std::string method;
        boost::json::value params;
    };

    MockWsConn(boost::beast::websocket::stream<boost::asio::ip::tcp::socket>& ws, std::string user_agent)
        : ws_(ws), user_agent_(std::move(user_agent)) {}

    boost::asio::awaitable<Req> read_req();                  // next text frame parsed as a JSON-RPC request
    boost::asio::awaitable<void> send(const std::string& text);
    boost::asio::awaitable<void> close(boost::beast::websocket::close_code code = boost::beast::websocket::close_code::going_away);
    void drop();                                             // hard TCP close, no WS close handshake
    boost::asio::awaitable<void> go_silent();                // stop reading AND writing (no pongs): a half-open peer
    boost::asio::awaitable<void> sleep(std::chrono::milliseconds d);
    const std::string& user_agent() const { return user_agent_; }

private:
    boost::beast::websocket::stream<boost::asio::ip::tcp::socket>& ws_;
    std::string user_agent_;
};

struct MockWsOptions {
    int reject_status{0};  // != 0 => answer the upgrade with this HTTP status instead of 101
    std::string reject_body{};
    std::vector<std::pair<std::string, std::string>> reject_headers{};
};

// Scripted in-process WebSocket server on 127.0.0.1:<ephemeral>, living on the caller's io_context.
class MockWsServer {
public:
    using Script = std::function<boost::asio::awaitable<void>(MockWsConn&)>;

    MockWsServer(boost::asio::io_context& ioc, Script script, MockWsOptions options = {});

    std::string url() const;  // ws://127.0.0.1:<port>/
    int connections() const;  // accepted TCP connections
    int upgrades() const;     // completed WS upgrades

    struct State;  // implementation detail, public only so the .cpp-local coroutines can name it

private:
    std::shared_ptr<State> st_;
};

}  // namespace evml::test
