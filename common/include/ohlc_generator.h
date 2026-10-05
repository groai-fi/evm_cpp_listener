#pragma once

#include <chrono>
#include <mutex>
#include <optional>
#include <unordered_map>

#include "types.h"

using namespace std;

class OHLCGenerator {
public:
    explicit OHLCGenerator(chrono::milliseconds interval) : interval_(interval), running_(true) {}

    optional<OHLC> process_tick(const MarketTick& tick);
    void stop() { running_ = false; }

private:
    struct SymbolState {
        OHLC current_ohlc;
        chrono::system_clock::time_point last_update;
        bool initialized = false;
    };

    chrono::milliseconds interval_;
    mutex mtx_;
    bool running_;
    unordered_map<string, SymbolState> symbol_states_;

    void reset_ohlc(SymbolState& state, const MarketTick& tick);
    bool should_emit_ohlc(const SymbolState& state, chrono::system_clock::time_point now) const;
};
