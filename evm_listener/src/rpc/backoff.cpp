#include "rpc/backoff.hpp"

#include <algorithm>
#include <stdexcept>

namespace evml::rpc {

Backoff::Backoff(std::uint64_t seed, ms base, ms cap, ms min_live)
    : base_(base), cap_(cap), min_live_(min_live), rng_(seed) {
    if (base_.count() <= 0) throw std::invalid_argument("Backoff: base must be > 0");
    if (cap_ < base_) throw std::invalid_argument("Backoff: cap must be >= base");
}

Backoff::ms Backoff::window() const {
    // Clamp the shift so base * 2^attempt cannot overflow int64 (cap is far below that anyway).
    const std::uint32_t shift = std::min<std::uint32_t>(attempt_, 40);
    const std::int64_t base = base_.count();
    const std::int64_t grown = (base > (cap_.count() >> shift)) ? cap_.count() : (base << shift);
    return ms(std::min<std::int64_t>(grown, cap_.count()));
}

Backoff::ms Backoff::next() {
    const auto w = window();
    std::uniform_int_distribution<std::int64_t> dist(0, w.count());
    ++attempt_;
    return ms(dist(rng_));
}

void Backoff::report_live(ms live_for) {
    if (live_for >= min_live_) attempt_ = 0;
}

}  // namespace evml::rpc
