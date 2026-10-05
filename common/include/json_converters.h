// src/json_converters.cpp
#pragma once

#include <chrono>
#include <iostream>
#include <stdexcept>  // For std::invalid_argument

#include "market_data.h"
#include "types.h"

// Undefine macros from system headers that conflict with our enum values
// These are defined in <arpa/nameser.h> which gets included via httplib.h
#ifdef QUERY
#undef QUERY
#endif
#ifdef ADD
#undef ADD
#endif

using json = nlohmann::json;
using namespace std;

// Helper functions for enum conversion

inline string to_string(ExchangeType type) {
    switch (type) {
        case ExchangeType::BINANCE:
            return "BINANCE";
        case ExchangeType::OKX:
            return "OKX";
        case ExchangeType::BYBIT:
            return "BYBIT";
        default:
            return "BINANCE";
    }
}

inline string to_string(OrderSide side) {
    switch (side) {
        case OrderSide::BUY:
            return "BUY";
        case OrderSide::SELL:
            return "SELL";
        default:
            cerr << "to_string(OrderSide side) default to BUY" << endl;
            return "BUY";
    }
}

inline string to_string(OrderType type) {
    switch (type) {
        case OrderType::MARKET:
            return "MARKET";
        case OrderType::LIMIT:
            return "LIMIT";
        default:
            cerr << "to_string(OrderType type) default to MARKET" << endl;
            return "MARKET";
    }
}

inline string to_string(ActionType action) {
    switch (action) {
        case ActionType::ORDER_REQUEST:
            return "ORDER_REQUEST";
        case ActionType::ORDER_STATUS_REQUEST:
            return "ORDER_STATUS_REQUEST";
        case ActionType::CANCEL_ORDER_REQUEST:
            return "CANCEL_ORDER_REQUEST";
        case ActionType::STOP_LOSS_REQUEST:
            return "STOP_LOSS_REQUEST";
        case ActionType::TAKE_PROFIT_REQUEST:
            return "TAKE_PROFIT_REQUEST";
        case ActionType::TRAILING_STOP_REQUEST:
            return "TRAILING_STOP_REQUEST";
        case ActionType::BALANCE_REQUEST:
            return "BALANCE_REQUEST";
        case ActionType::LEVERAGE_REQUEST:
            return "LEVERAGE_REQUEST";
        case ActionType::LEVERAGE_STATUS_REQUEST:
            return "LEVERAGE_STATUS_REQUEST";
        case ActionType::HEALTH_CHECK_REQUEST:
            return "HEALTH_CHECK_REQUEST";
        case ActionType::EXCHANGE_STATUS_REQUEST:
            return "EXCHANGE_STATUS_REQUEST";
        case ActionType::OPEN_ORDERS_REQUEST:
            return "OPEN_ORDERS_REQUEST";
        case ActionType::POSITION_REQUEST:
            return "POSITION_REQUEST";
        case ActionType::OCO_ORDER_REQUEST:
            return "OCO_ORDER_REQUEST";
        case ActionType::OCO_UPDATE_FROM_WS:
            return "OCO_UPDATE_FROM_WS";
        default:
            cerr << "to_string(ActionType action) default to UNKNOWN ActionType" << endl;
            return "UNKNOWN";
    }
}

inline string to_string(ActionRespType action) {
    switch (action) {
        case ActionRespType::ORDER_RESPONSE:
            return "ORDER_RESPONSE";
        case ActionRespType::LEVERAGE_RESPONSE:
            return "LEVERAGE_RESPONSE";
        case ActionRespType::BALANCE_RESPONSE:
            return "BALANCE_RESPONSE";
        case ActionRespType::ERROR_RESPONSE:
            return "ERROR_RESPONSE";
        case ActionRespType::HEALTH_CHECK_RESPONSE:
            return "HEALTH_CHECK_RESPONSE";
        case ActionRespType::EXCHANGE_STATUS_RESPONSE:
            return "EXCHANGE_STATUS_RESPONSE";
        case ActionRespType::ORDER_STATUS_RESPONSE:
            return "ORDER_STATUS_RESPONSE";
        case ActionRespType::OPEN_ORDER_RESPONSE:
            return "OPEN_ORDER_RESPONSE";
        case ActionRespType::POSITION_RESPONSE:
            return "POSITION_RESPONSE";
        default:
            cerr << "to_string(ActionRespType action) UNKNOWN ActionRespType" << endl;
            return "UNKNOWN";
    }
}

inline string to_string(ActionMethod method) {
    switch (method) {
        case ActionMethod::CREATE:
            return "create";
        case ActionMethod::CANCEL:
            return "cancel";
        case ActionMethod::UPDATE:
            return "update";
        case ActionMethod::QUERY:
            return "query";
        case ActionMethod::ADD:
            return "add";
        case ActionMethod::REMOVE:
            return "remove";
        case ActionMethod::MODIFY:
            return "modify";
        default:
            cerr << "to_string(ActionMethod method) UNKNOWN ActionMethod" << endl;
            return "UNKNOWN";
    }
}

inline string to_string(MarketType market) {
    switch (market) {
        case MarketType::SPOT:
            return "spot";
        case MarketType::FUTURES_USDT:
            return "futures_usdt";
        case MarketType::FUTURES_COIN:
            return "futures_coin";
        case MarketType::MARGIN:
            return "margin";
        default:
            cerr << "to_string(MarketType market) default to futures_usdt" << endl;
            return "futures_usdt";
    }
}

inline string to_string(TimeInForce tif) {
    switch (tif) {
        case TimeInForce::GTC:
            return "GTC";
        case TimeInForce::IOC:
            return "IOC";
        case TimeInForce::FOK:
            return "FOK";
        case TimeInForce::GTX:
            return "GTX";
        default:
            cerr << "to_string(TimeInForce tif) default to GTC" << endl;
            return "GTC";
    }
}

inline string to_string(PositionSide pos) {
    switch (pos) {
        case PositionSide::BOTH:
            return "BOTH";
        case PositionSide::LONG:
            return "LONG";
        case PositionSide::SHORT:
            return "SHORT";
        default:
            cerr << "to_string(PositionSide pos) default to BOTH" << endl;
            return "BOTH";
    }
}

// Added PositionMode to_string function
inline string to_string(PositionMode mode) {
    switch (mode) {
        case PositionMode::ONE_WAY:
            return "ONE_WAY";
        case PositionMode::DUAL_SIDE:
            return "DUAL_SIDE";
        default:
            cerr << "to_string(PositionMode mode) default to ONE_WAY" << endl;
            return "ONE_WAY";
    }
}

//
inline string to_string(TradeEnv trade_env) {
    switch (trade_env) {
        case TradeEnv::DUMMY:
            return "DUMMY";
        case TradeEnv::TEST_NET:
            return "TEST_NET";
        case TradeEnv::LIVE:
            return "LIVE";
        default:
            cerr << "to_string(TradeEnv trade_env) default to DUMMY" << endl;
            return "DUMMY";
    }
}

inline string to_string(TradingSignal::Type type) {
    switch (type) {
        case TradingSignal::Type::BUY:
            return "BUY";
        case TradingSignal::Type::SELL:
            return "SELL";
        case TradingSignal::Type::EXIT_LONG:
            return "EXIT_LONG";
        default:
            return "UNKNOWN";
    }
}

inline string to_string(TradingSignal::Timeframe tf) {
    switch (tf) {
        case TradingSignal::Timeframe::H1:
            return "H1";
        case TradingSignal::Timeframe::H4:
            return "H4";
        case TradingSignal::Timeframe::M10:
            return "M10";
        case TradingSignal::Timeframe::M40:
            return "M40";
        default:
            return "UNKNOWN";
    }
}

template <typename T>
T from_string(const string& str);

template <>
inline PositionSide from_string<PositionSide>(const string& str) {
    if (str == "BOTH") return PositionSide::BOTH;
    if (str == "LONG") return PositionSide::LONG;
    if (str == "SHORT") return PositionSide::SHORT;
    throw std::invalid_argument("Invalid position side: " + str);
}

template <>
inline ActionRespType from_string<ActionRespType>(const string& str) {
    if (str == "ORDER_RESPONSE") return ActionRespType::ORDER_RESPONSE;
    if (str == "LEVERAGE_RESPONSE") return ActionRespType::LEVERAGE_RESPONSE;
    if (str == "BALANCE_RESPONSE") return ActionRespType::BALANCE_RESPONSE;
    if (str == "ERROR_RESPONSE") return ActionRespType::ERROR_RESPONSE;
    if (str == "HEALTH_CHECK_RESPONSE") return ActionRespType::HEALTH_CHECK_RESPONSE;
    if (str == "EXCHANGE_STATUS_RESPONSE") return ActionRespType::EXCHANGE_STATUS_RESPONSE;
    if (str == "ORDER_STATUS_RESPONSE") return ActionRespType::ORDER_STATUS_RESPONSE;
    if (str == "OPEN_ORDER_RESPONSE") return ActionRespType::OPEN_ORDER_RESPONSE;
    if (str == "POSITION_RESPONSE") return ActionRespType::POSITION_RESPONSE;
    throw std::invalid_argument("Invalid action response type: " + str);
}

template <>
inline ExchangeType from_string<ExchangeType>(const string& str) {
    if (str == "OKX") return ExchangeType::OKX;
    if (str == "BYBIT") return ExchangeType::BYBIT;
    if (str == "BINANCE") return ExchangeType::BINANCE;
    throw std::invalid_argument("Invalid exchange type: " + str);
}

template <>
inline MarketType from_string<MarketType>(const string& str) {
    if (str == "SPOT" || str == "spot") return MarketType::SPOT;
    if (str == "FUTURES_COIN" || str == "futures_coin") return MarketType::FUTURES_COIN;
    if (str == "FUTURES_USDT" || str == "futures_usdt") return MarketType::FUTURES_USDT;
    if (str == "MARGIN" || str == "margin") return MarketType::MARGIN;
    throw std::invalid_argument("Invalid market type: " + str);
}

template <>
inline TradeEnv from_string<TradeEnv>(const string& str) {
    if (str.empty() || str == "DUMMY") return TradeEnv::DUMMY;
    if (str == "TEST_NET") return TradeEnv::TEST_NET;
    if (str == "LIVE") return TradeEnv::LIVE;
    throw std::invalid_argument("Invalid TradeEnv: " + str);
}

template <>
inline OrderSide from_string<OrderSide>(const string& str) {
    if (str == "BUY" || str == "buy") return OrderSide::BUY;
    if (str == "SELL" || str == "sell") return OrderSide::SELL;
    throw std::invalid_argument("Invalid order side type: " + str);
}

template <>
inline OrderType from_string<OrderType>(const string& str) {
    if (str == "MARKET" || str == "market") return OrderType::MARKET;
    if (str == "LIMIT" || str == "limit") return OrderType::LIMIT;
    throw std::invalid_argument("Invalid order type: " + str);
}

// JSON conversion functions
inline json to_json(const OrderRequest& req) {
    nlohmann::json j;
    j["request_id"] = req.request_id;
    if (req.client_order_id) j["client_order_id"] = *req.client_order_id;
    j["exchange_type"] = to_string(req.exchange_type);
    j["market_type"] = to_string(req.market_type);
    j["is_test"] = req.is_test;
    j["position_mode"] = to_string(req.position_mode);  // Added position_mode
    j["timestamp"] = req.timestamp;
    j["action_type"] = to_string(req.action_type);
    j["action_method"] = to_string(req.action_method);
    j["symbol"] = req.symbol;
    j["side"] = to_string(req.side);
    j["order_type"] = to_string(req.order_type);
    j["time_in_force"] = to_string(req.time_in_force);

    if (req.quantity) j["quantity"] = *req.quantity;
    if (req.price) j["price"] = *req.price;
    if (req.stop_price) j["stop_price"] = *req.stop_price;
    if (req.position_side) j["position_side"] = to_string(*req.position_side);
    if (req.reduce_only) j["reduce_only"] = *req.reduce_only;  // Changed to conditional
    if (req.exchange_order_id) j["exchange_order_id"] = *req.exchange_order_id;
    if (req.return_topic) j["return_topic"] = *req.return_topic;

    return j;
}

inline json to_json(const CancelOrderRequest& req) {
    nlohmann::json j;
    j["request_id"] = req.request_id;
    if (req.client_order_id) j["client_order_id"] = *req.client_order_id;
    j["exchange_type"] = to_string(req.exchange_type);
    j["market_type"] = to_string(req.market_type);
    j["is_test"] = req.is_test;
    j["position_mode"] = to_string(req.position_mode);
    j["timestamp"] = req.timestamp;
    j["action_type"] = to_string(req.action_type);
    j["action_method"] = to_string(req.action_method);
    j["symbol"] = req.symbol;
    j["exchange_order_id"] = req.exchange_order_id;

    if (req.return_topic) j["return_topic"] = *req.return_topic;

    return j;
}

inline json to_json(const StopLossRequest& req) {
    nlohmann::json j;
    j["request_id"] = req.request_id;
    if (req.client_order_id) j["client_order_id"] = *req.client_order_id;
    j["exchange_type"] = to_string(req.exchange_type);
    j["market_type"] = to_string(req.market_type);
    j["is_test"] = req.is_test;
    j["position_mode"] = to_string(req.position_mode);
    j["timestamp"] = req.timestamp;
    j["action_type"] = to_string(req.action_type);
    j["action_method"] = to_string(req.action_method);
    j["symbol"] = req.symbol;
    j["side"] = to_string(req.side);
    j["stop_price"] = req.stop_price;  // Now required field
    j["order_type"] = to_string(req.order_type);
    j["time_in_force"] = to_string(req.time_in_force);

    if (req.quantity) j["quantity"] = *req.quantity;
    if (req.reduce_only) j["reduce_only"] = *req.reduce_only;
    if (req.position_side) j["position_side"] = to_string(*req.position_side);
    if (req.exchange_order_id) j["exchange_order_id"] = *req.exchange_order_id;
    if (req.parent_order_id) j["parent_order_id"] = *req.parent_order_id;
    if (req.return_topic) j["return_topic"] = *req.return_topic;

    return j;
}

inline json to_json(const TakeProfitRequest& req) {
    nlohmann::json j;
    j["request_id"] = req.request_id;
    if (req.client_order_id) j["client_order_id"] = *req.client_order_id;
    j["exchange_type"] = to_string(req.exchange_type);
    j["market_type"] = to_string(req.market_type);
    j["is_test"] = req.is_test;
    j["position_mode"] = to_string(req.position_mode);
    j["timestamp"] = req.timestamp;
    j["action_type"] = to_string(req.action_type);
    j["action_method"] = to_string(req.action_method);
    j["symbol"] = req.symbol;
    j["side"] = to_string(req.side);
    j["take_profit_price"] = req.take_profit_price;  // Changed from stop_price
    j["order_type"] = to_string(req.order_type);
    j["time_in_force"] = to_string(req.time_in_force);

    if (req.price) j["price"] = *req.price;
    if (req.quantity) j["quantity"] = *req.quantity;
    if (req.reduce_only) j["reduce_only"] = *req.reduce_only;
    if (req.position_side) j["position_side"] = to_string(*req.position_side);
    if (req.exchange_order_id) j["exchange_order_id"] = *req.exchange_order_id;
    if (req.parent_order_id) j["parent_order_id"] = *req.parent_order_id;
    if (req.return_topic) j["return_topic"] = *req.return_topic;

    return j;
}

inline json to_json(const TrailingStopRequest& req) {
    nlohmann::json j;
    j["request_id"] = req.request_id;
    if (req.client_order_id) j["client_order_id"] = *req.client_order_id;
    j["exchange_type"] = to_string(req.exchange_type);
    j["market_type"] = to_string(req.market_type);
    j["is_test"] = req.is_test;
    j["position_mode"] = to_string(req.position_mode);
    j["timestamp"] = req.timestamp;
    j["action_type"] = to_string(req.action_type);
    j["action_method"] = to_string(req.action_method);
    j["symbol"] = req.symbol;
    j["side"] = to_string(req.side);

    // Trailing stop specific fields
    j["callback_rate"] = req.callback_rate;  // Required: trailing percentage
    if (req.activation_price)
        j["activation_price"] = *req.activation_price;  // Optional: activation price

    j["order_type"] = to_string(req.order_type);
    j["time_in_force"] = to_string(req.time_in_force);

    // Optional fields
    if (req.quantity) j["quantity"] = *req.quantity;
    if (req.reduce_only) j["reduce_only"] = *req.reduce_only;
    if (req.position_side) j["position_side"] = to_string(*req.position_side);
    if (req.exchange_order_id) j["exchange_order_id"] = *req.exchange_order_id;
    if (req.parent_request_id)
        j["parent_request_id"] = *req.parent_request_id;  // Note: parent_request_id
    if (req.return_topic) j["return_topic"] = *req.return_topic;

    return j;
}

inline json to_json(const BalanceRequest& req) {
    nlohmann::json j;
    j["request_id"] = req.request_id;
    if (req.client_order_id) j["client_order_id"] = *req.client_order_id;
    j["exchange_type"] = to_string(req.exchange_type);
    j["market_type"] = to_string(req.market_type);
    j["is_test"] = req.is_test;
    if (req.return_topic) j["return_topic"] = *req.return_topic;
    j["position_mode"] = to_string(req.position_mode);
    j["timestamp"] = req.timestamp;
    j["action_type"] = to_string(req.action_type);
    j["action_method"] = to_string(req.action_method);
    if (req.asset) j["asset"] = *req.asset;

    return j;
}

inline json to_json(const LeverageRequest& req) {
    nlohmann::json j;
    j["request_id"] = req.request_id;
    if (req.client_order_id) j["client_order_id"] = *req.client_order_id;
    j["exchange_type"] = to_string(req.exchange_type);
    j["market_type"] = to_string(req.market_type);
    j["is_test"] = req.is_test;
    j["position_mode"] = to_string(req.position_mode);
    j["timestamp"] = req.timestamp;
    j["action_type"] = to_string(req.action_type);
    j["action_method"] = to_string(req.action_method);
    j["symbol"] = req.symbol;
    j["leverage"] = req.leverage;

    if (req.return_topic) j["return_topic"] = *req.return_topic;

    return j;
}

inline json to_json(const OpenOrdersRequest& req) {
    nlohmann::json j;
    j["request_id"] = req.request_id;
    if (req.client_order_id) j["client_order_id"] = *req.client_order_id;
    j["exchange_type"] = to_string(req.exchange_type);
    j["market_type"] = to_string(req.market_type);
    j["is_test"] = req.is_test;
    j["position_mode"] = to_string(req.position_mode);
    j["timestamp"] = req.timestamp;
    j["action_type"] = to_string(req.action_type);
    j["action_method"] = to_string(req.action_method);

    if (req.symbol) j["symbol"] = *req.symbol;
    if (req.return_topic) j["return_topic"] = *req.return_topic;

    return j;
}

inline json to_json(const OrderStatusRequest& req) {
    nlohmann::json j;
    j["request_id"] = req.request_id;
    if (req.client_order_id) j["client_order_id"] = *req.client_order_id;
    j["exchange_type"] = to_string(req.exchange_type);
    j["market_type"] = to_string(req.market_type);
    j["is_test"] = req.is_test;
    j["position_mode"] = to_string(req.position_mode);
    j["timestamp"] = req.timestamp;
    j["action_type"] = to_string(req.action_type);
    j["action_method"] = to_string(req.action_method);
    j["symbol"] = req.symbol;

    if (req.exchange_order_id) j["exchange_order_id"] = *req.exchange_order_id;
    // Note: client_order_id in request body is different from the base client_order_id
    if (req.client_order_id) j["client_order_id"] = *req.client_order_id;
    if (req.return_topic) j["return_topic"] = *req.return_topic;

    return j;
}

inline json to_json(const PositionRequest& req) {
    json j;
    j["request_id"] = req.request_id;
    if (req.client_order_id) j["client_order_id"] = *req.client_order_id;
    j["exchange_type"] = to_string(req.exchange_type);
    j["market_type"] = to_string(req.market_type);
    j["is_test"] = req.is_test;
    j["position_mode"] = to_string(req.position_mode);
    j["timestamp"] = req.timestamp;
    j["action_type"] = to_string(req.action_type);
    j["action_method"] = to_string(req.action_method);

    if (req.symbol) j["symbol"] = *req.symbol;
    if (req.return_topic) j["return_topic"] = *req.return_topic;

    return j;
}

inline json to_json(const WsUpdatePayload& req) {
    json j;
    j["request_id"] = req.request_id;
    j["exchange_type"] = to_string(req.exchange_type);
    j["market_type"] = to_string(req.market_type);
    j["status"] = req.status;
    j["action_type"] = to_string(req.action_type);
    j["action_method"] = to_string(req.action_method);
    j["symbol"] = req.symbol;

    if (req.client_order_id) j["client_order_id"] = *req.client_order_id;
    if (req.exchange_order_id) j["exchange_order_id"] = *req.exchange_order_id;
    if (req.cum_filled_qty) j["cum_filled_qty"] = *req.cum_filled_qty;
    if (req.average_price) j["average_price"] = *req.average_price;
    if (req.fee) j["fee"] = *req.fee;
    if (req.order_type) j["order_type"] = *req.order_type;
    if (req.order_qty) j["order_qty"] = *req.order_qty;

    return j;
}

inline json to_json(const OHLC& req) {
    json j;
    j["symbol"] = req.symbol;
    j["timestamp"] = req.timestamp;
    j["open"] = req.open;
    j["high"] = req.high;
    j["low"] = req.low;
    j["close"] = req.close;
    j["volume"] = req.volume;
    j["timeframe"] = req.timeframe;

    return j;
}

inline json to_json(const MarketTick& req) {
    json j;
    j["symbol"] = req.symbol;
    j["price"] = req.price;
    j["volume"] = req.volume;
    j["timestamp"] = req.timestamp;

    return j;
}

inline json to_json(const TradingSignal& signal) {
    nlohmann::json j;

    j["symbol"] = signal.symbol;
    j["signal_type"] = to_string(signal.signal_type);
    j["timeframe"] = to_string(signal.timeframe);

    // Use exact types for numbers (doubles/int64)
    j["price"] = signal.price;
    j["stop_loss"] = signal.stop_loss;
    j["take_profit"] = signal.take_profit;
    j["activation_price_pct"] = signal.activation_price_pct;
    j["callback_rate"] = signal.callback_rate;

    j["reason"] = signal.reason;
    j["timestamp"] = signal.timestamp;

    return j;
}
