#pragma once
#include <chrono>
#include <deque>
#include <nlohmann/json.hpp>
#include <string>

#include "types.h"

using namespace std;

struct MarketData {
    string symbol;
    double price;
    double volume;
    int64_t timestamp;

    NLOHMANN_DEFINE_TYPE_INTRUSIVE(MarketData, symbol, price, volume)
};

struct TradingSignal {
    enum class Type { BUY, SELL, EXIT_LONG, EXIT_SHORT };
    enum class Timeframe { H1, H4, M10, M40 };

    string symbol;
    Type signal_type;
    Timeframe timeframe;
    double price;
    double stop_loss;
    double take_profit;
    double activation_price_pct;
    double callback_rate;
    string reason;
    int64_t timestamp;

    NLOHMANN_DEFINE_TYPE_INTRUSIVE(TradingSignal, symbol, signal_type, timeframe, price, stop_loss,
                                   take_profit, activation_price_pct, callback_rate, reason)
};

NLOHMANN_JSON_SERIALIZE_ENUM(TradingSignal::Type, {{TradingSignal::Type::BUY, "BUY"},
                                                   {TradingSignal::Type::SELL, "SELL"},
                                                   {TradingSignal::Type::EXIT_LONG, "EXIT_LONG"},
                                                   {TradingSignal::Type::EXIT_SHORT, "EXIT_SHORT"}})

NLOHMANN_JSON_SERIALIZE_ENUM(TradingSignal::Timeframe, {{TradingSignal::Timeframe::H1, "H1"},
                                                        {TradingSignal::Timeframe::H4, "H4"},
                                                        {TradingSignal::Timeframe::M10, "M10"},
                                                        {TradingSignal::Timeframe::M40, "M40"}})
