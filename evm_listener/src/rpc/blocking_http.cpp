#include "rpc/blocking_http.hpp"

#include <utility>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_future.hpp>

#include "ops/http_client.hpp"

namespace evml::rpc {

namespace {

boost::asio::awaitable<HttpResponse> post(ops::HttpClient& client, const std::string& url, const std::string& body) {
    ops::HttpClient::Headers headers{{"Accept", "application/json"}};
    auto r = co_await client.post_json(url, body, headers);
    co_return HttpResponse{r.status, std::move(r.body)};
}

}  // namespace

/// @brief Synchronously runs HTTP POST via ops::HttpClient on a temporary io_context.
HttpResponse http_post_json(const std::string& url, const std::string& body, std::chrono::milliseconds timeout,
                            const std::string& user_agent) {
    namespace net = boost::asio;
    net::io_context ioc;
    ops::HttpClient client(ioc.get_executor(), ops::HttpClientOptions{.user_agent = user_agent, .timeout = timeout});
    auto fut = net::co_spawn(ioc, post(client, url, body), net::use_future);
    ioc.run();
    return fut.get();
}

}  // namespace evml::rpc
