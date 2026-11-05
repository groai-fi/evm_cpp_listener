#include "rpc/blocking_http.hpp"

#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_future.hpp>
#include <utility>

#include "ops/http_client.hpp"

namespace evml::rpc {

namespace {

boost::asio::awaitable<HttpResponse> post(ops::HttpClient client, std::string url, std::string body) {
    ops::HttpClient::Headers headers{{"Accept", "application/json"}};
    auto r = co_await client.post_json(std::move(url), std::move(body), std::move(headers));
    co_return HttpResponse{r.status, std::move(r.body)};
}

}  // namespace

/// @brief Synchronously runs HTTP POST via ops::HttpClient on a temporary io_context.
HttpResponse http_post_json(std::string_view url, std::string_view body, std::chrono::milliseconds timeout,
                            std::string_view user_agent) {
    namespace net = boost::asio;
    net::io_context ioc;
    ops::HttpClient client(ioc.get_executor(),
                           ops::HttpClientOptions{.user_agent = std::string(user_agent), .timeout = timeout});
    auto fut = net::co_spawn(ioc, post(std::move(client), std::string(url), std::string(body)), net::use_future);
    ioc.run();
    return fut.get();
}

}  // namespace evml::rpc
