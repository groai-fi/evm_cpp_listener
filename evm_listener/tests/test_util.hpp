#pragma once

#include <chrono>
#include <exception>
#include <optional>
#include <utility>

#include <boost/asio.hpp>
#include <catch2/catch_test_macros.hpp>

namespace evml::test {

// Runs one coroutine to completion on `ioc` (which may host never-ending servers), failing the test if it does
// not finish within `limit`. Rethrows the coroutine's exception.
template <class T>
T run_sync(boost::asio::io_context& ioc, boost::asio::awaitable<T> a,
           std::chrono::milliseconds limit = std::chrono::seconds(15)) {
    std::optional<T> out;
    std::exception_ptr err;
    bool done = false;
    boost::asio::co_spawn(ioc, std::move(a), [&](std::exception_ptr e, T v) {
        err = e;
        if (!e) out = std::move(v);
        done = true;
        ioc.stop();
    });
    ioc.restart();
    ioc.run_for(limit);
    REQUIRE(done);
    if (err) std::rethrow_exception(err);
    return std::move(*out);
}

}  // namespace evml::test
