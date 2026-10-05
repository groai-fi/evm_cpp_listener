#include "ohlc_generator.h"

#include <algorithm>

optional<OHLC> OHLCGenerator::process_tick(const MarketTick& tick) {
    scoped_lock<mutex> lock(mtx_);

    if (!running_) return nullopt;

    auto now = chrono::system_clock::now();
    auto& state = symbol_states_[tick.symbol];

    // Initialize if first tick for this symbol
    if (!state.initialized) {
        reset_ohlc(state, tick);
        state.last_update = now;
        state.initialized = true;
        return nullopt;
    }

    // Check if we should emit current OHLC and start new one
    if (should_emit_ohlc(state, now)) {
        // Create result from current state
        OHLC result = state.current_ohlc;
        result.timeframe = to_string(interval_.count()) + "ms";

        // Reset for next interval
        reset_ohlc(state, tick);
        state.last_update = now;

        return result;
    }

    // Update current OHLC with new tick
    state.current_ohlc.high = max(state.current_ohlc.high, tick.price);
    state.current_ohlc.low = min(state.current_ohlc.low, tick.price);
    state.current_ohlc.close = tick.price;
    state.current_ohlc.volume += tick.volume;

    return nullopt;
}

void OHLCGenerator::reset_ohlc(SymbolState& state, const MarketTick& tick) {
    state.current_ohlc.symbol = tick.symbol;
    state.current_ohlc.timestamp = tick.timestamp;
    state.current_ohlc.open = tick.price;
    state.current_ohlc.high = tick.price;
    state.current_ohlc.low = tick.price;
    state.current_ohlc.close = tick.price;
    state.current_ohlc.volume = tick.volume;
}

bool OHLCGenerator::should_emit_ohlc(const SymbolState& state,
                                     chrono::system_clock::time_point now) const {
    auto elapsed = chrono::duration_cast<chrono::milliseconds>(now - state.last_update);
    return elapsed >= interval_;
}
