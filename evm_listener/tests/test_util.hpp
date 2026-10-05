#pragma once

#include <chrono>
#include <exception>
#include <functional>
#include <memory>
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

// Runs `ioc` until `pred()` is true or `limit` elapses; returns the final value of pred(). `pred` runs only
// while this function runs (a stale timer callback from an earlier call is neutralised by the `dead` flag).
inline bool run_until(boost::asio::io_context& ioc, std::function<bool()> pred, std::chrono::milliseconds limit) {
    struct Poller : std::enable_shared_from_this<Poller> {
        Poller(boost::asio::io_context& i, std::function<bool()> p, std::chrono::steady_clock::time_point e)
            : ioc(i), timer(i), pred(std::move(p)), end(e) {}
        boost::asio::io_context& ioc;
        boost::asio::steady_timer timer;
        std::function<bool()> pred;
        std::chrono::steady_clock::time_point end;
        bool dead{false};
        void tick() {
            timer.async_wait([self = this->shared_from_this()](const boost::system::error_code& ec) {
                if (ec || self->dead) return;
                if (self->pred() || std::chrono::steady_clock::now() >= self->end) {
                    self->ioc.stop();
                    return;
                }
                self->timer.expires_after(std::chrono::milliseconds(5));
                self->tick();
            });
        }
    };
    auto p = std::make_shared<Poller>(ioc, pred, std::chrono::steady_clock::now() + limit);
    ioc.restart();
    p->timer.expires_after(std::chrono::milliseconds(0));
    p->tick();
    ioc.run();
    p->dead = true;
    p->timer.cancel();
    return pred();
}

}  // namespace evml::test
