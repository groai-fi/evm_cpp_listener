// include/types.h
#pragma once
#include <nlohmann/json.hpp>
#include <optional>
#include <string>

using namespace std;

// Enums matching Python schema
enum class ExchangeType { BINANCE, OKX, BYBIT };

enum class OrderSide { BUY, SELL };

enum class OrderType {
    MARKET,
    LIMIT
//    STOP_MARKET,
//    STOP,
//    TAKE_PROFIT_MARKET,
//    TAKE_PROFIT,
//    STOP_LOSS_LIMIT,
//    TAKE_PROFIT_LIMIT,
//    TRAILING_STOP_MARKET
};

enum class ActionType {
    // Request types
    ORDER_REQUEST,
    ORDER_STATUS_REQUEST,
    CANCEL_ORDER_REQUEST,
    STOP_LOSS_REQUEST,
    TAKE_PROFIT_REQUEST,
    TRAILING_STOP_REQUEST,
    BALANCE_REQUEST,
    LEVERAGE_REQUEST,
    LEVERAGE_STATUS_REQUEST,
    HEALTH_CHECK_REQUEST,
    EXCHANGE_STATUS_REQUEST,
    OPEN_ORDERS_REQUEST,
    POSITION_REQUEST,
    OCO_ORDER_REQUEST,
    OCO_UPDATE_FROM_WS
};

enum class ActionRespType {
    // Response types
    ORDER_RESPONSE,
    LEVERAGE_RESPONSE,
    BALANCE_RESPONSE,
    ERROR_RESPONSE,
    HEALTH_CHECK_RESPONSE,
    EXCHANGE_STATUS_RESPONSE,
    ORDER_STATUS_RESPONSE,
    OPEN_ORDER_RESPONSE,
    POSITION_RESPONSE
};

enum class ActionMethod {
    // Order actions
    CREATE,
    CANCEL,
    UPDATE,
    QUERY,

    // Stop loss / Take profit actions
    ADD,
    REMOVE,
    MODIFY,

    // Legacy support, to be removed
//    GET_BALANCE,
//    GET_LEVERAGE,
//    SET_LEVERAGE,
//    GET_POSITION
};

enum class MarketType { SPOT, FUTURES_USDT, FUTURES_COIN, MARGIN };

enum class TimeInForce { GTC, IOC, FOK, GTX };

enum class PositionSide { BOTH, LONG, SHORT };

// Added PositionMode enum to match Python schema
enum class PositionMode { ONE_WAY, DUAL_SIDE };

enum class TradeEnv { DUMMY, TEST_NET, LIVE };

// Base request structure - Updated to match Python BaseRequest
struct BaseRequest {
    string request_id;
    optional<string> client_order_id = nullopt;
    ExchangeType exchange_type;
    MarketType market_type;
    bool is_test = true;
    optional<string> return_topic;
    PositionMode position_mode = PositionMode::ONE_WAY;
    int64_t timestamp;
};

// Order request - Updated to match Python OrderRequest
struct OrderRequest : BaseRequest {
    ActionType action_type = ActionType::ORDER_REQUEST;
    ActionMethod action_method;
    string symbol;
    OrderSide side;
    OrderType order_type;
    optional<string> quantity;
    optional<string> price;
    optional<string> stop_price;
    TimeInForce time_in_force = TimeInForce::GTC;
    optional<PositionSide> position_side;
    optional<bool> reduce_only;
    optional<string> exchange_order_id;
};

struct CancelOrderRequest : BaseRequest {
    ActionType action_type = ActionType::CANCEL_ORDER_REQUEST;
    ActionMethod action_method = ActionMethod::CANCEL;
    string symbol;
    string exchange_order_id;
};

// Stop loss request
struct StopLossRequest : BaseRequest {
    ActionType action_type = ActionType::STOP_LOSS_REQUEST;
    ActionMethod action_method;
    string symbol;
    OrderSide side;
    string stop_price;
    optional<string> quantity;
    OrderType order_type = OrderType::MARKET;
    TimeInForce time_in_force = TimeInForce::GTC;
    optional<bool> reduce_only;
    optional<PositionSide> position_side;
    optional<string> exchange_order_id;
    optional<string> parent_order_id;
};

// Take profit request
struct TakeProfitRequest : BaseRequest {
    ActionType action_type = ActionType::TAKE_PROFIT_REQUEST;
    ActionMethod action_method;
    string symbol;
    OrderSide side;
    string take_profit_price;
    optional<string> price;
    optional<string> quantity;
    OrderType order_type = OrderType::MARKET;
    TimeInForce time_in_force = TimeInForce::GTC;
    optional<bool> reduce_only;
    optional<PositionSide> position_side;
    optional<string> exchange_order_id;
    optional<string> parent_order_id;
};

// First, add the TrailingStopRequest struct (you'll need to add this to your headers)
struct TrailingStopRequest : BaseRequest {
    ActionType action_type = ActionType::TRAILING_STOP_REQUEST;
    ActionMethod action_method;
    string symbol;
    OrderSide side;
    double callback_rate;
    optional<double> activation_price;
    optional<string> quantity;
    OrderType order_type = OrderType::MARKET;
    TimeInForce time_in_force = TimeInForce::GTC;
    optional<bool> reduce_only;
    optional<PositionSide> position_side;
    optional<string> exchange_order_id;
    optional<string> parent_request_id;  // Note: parent_request_id instead of parent_order_id
};

// Balance request
struct BalanceRequest: BaseRequest {
    ActionType action_type = ActionType::BALANCE_REQUEST;
    ActionMethod action_method = ActionMethod::QUERY;
    optional<string> asset;  // Specific asset to query, or nullopt for all
};

// Leverage request
struct LeverageRequest : BaseRequest {
    ActionType action_type = ActionType::LEVERAGE_REQUEST;
    ActionMethod action_method = ActionMethod::UPDATE;
    string symbol;
    int leverage;
};

// Open orders request
struct OpenOrdersRequest : BaseRequest {
    ActionType action_type = ActionType::OPEN_ORDERS_REQUEST;
    ActionMethod action_method = ActionMethod::QUERY;
    optional<string> symbol;
};

// Order status request
struct OrderStatusRequest : BaseRequest {
    ActionType action_type = ActionType::ORDER_STATUS_REQUEST;
    ActionMethod action_method = ActionMethod::QUERY;
    string symbol;
    optional<string> exchange_order_id;
    optional<string> client_order_id;
};

// Position request
struct PositionRequest : BaseRequest {
    ActionType action_type = ActionType::POSITION_REQUEST;
    ActionMethod action_method = ActionMethod::QUERY;
    optional<string> symbol;
};

// Individual position data
struct PositionData {
    string symbol;
    PositionSide position_side;
    double size;
    double notional;
    double unrealized_pnl;
    double entry_price;
    double mark_price;
    double percentage;
    optional<string> margin_type;
    optional<double> isolated_margin;
};

// Base response structure
struct BaseResponse {
    string request_id;
    bool success;
    optional<string> client_order_id;
    ExchangeType exchange_type;
    MarketType market_type;
    optional<string> error_code;
    optional<string> error_message;
    int64_t timestamp = 0;
};

// Position response
struct PositionResponse : BaseResponse {
    ActionRespType action_resp_type = ActionRespType::POSITION_RESPONSE;  // Added this
    // Position summary
    bool has_long;
    bool has_short;
    double long_amount;
    double short_amount;
    double total_unrealized_pnl;

    // Detailed position data
    vector<PositionData> positions;

    // Raw exchange response (optional)
    optional<string> raw_response_json;
};

struct OrderResponse : BaseResponse {
    ActionRespType action_resp_type;

    // Order identification
    optional<string> exchange_order_id;
    string symbol;

    // Order details
    OrderSide side;
    OrderType order_type;
    optional<double> quantity;
    optional<double> price;
    optional<double> stop_price;
    optional<double> take_profit_price;

    // Order status
    optional<string> status;  // You might want to create an OrderStatus enum
    optional<double> filled_quantity;
    optional<double> remaining_quantity;
    optional<double> average_price;

    // Fees and commissions
    optional<double> commission;
    optional<string> commission_asset;

    // Timestamps (milliseconds since epoch)
    int64_t order_time = 0;      // Order creation timestamp
    int64_t update_time = 0;     // Last update timestamp
    int64_t last_trade_time = 0; // Last trade execution timestamp

    // Raw exchange response
    optional<nlohmann::json> raw_response;
};


struct WsUpdatePayload {
    string request_id;
    ActionType action_type = ActionType::OCO_UPDATE_FROM_WS;
    ActionMethod action_method = ActionMethod::UPDATE;
    ExchangeType exchange_type;
    MarketType market_type;
    string symbol;
    optional<string> client_order_id;
    optional<string> exchange_order_id;
    string status;
    optional<string> fee;
    optional<string> order_type;
    optional<string> order_qty;
    optional<string> cum_filled_qty;
    optional<string> average_price;

    optional<nlohmann::json> raw_response;
};

struct OHLC {
    string symbol;
    int64_t timestamp{};  // milliseconds since epoch
    double open{};
    double high{};
    double low{};
    double close{};
    double volume{};
    string timeframe;  // "1m", "1h", "4h", etc.
};

struct MarketTick {
    string symbol;
    double price;
    double volume;
    int64_t timestamp;
};