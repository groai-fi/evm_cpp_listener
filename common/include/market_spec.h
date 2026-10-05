#pragma once

#include <chrono>
#include <nlohmann/json.hpp>
#include <optional>
#include <string>
#include <unordered_map>

using json = nlohmann::json;

struct MarketSpecification {
    std::string symbol;
    std::optional<int> price_precision;
    std::optional<int> amount_precision;
    std::optional<double> min_amount;
    std::optional<double> min_notional;
    std::string base_asset;
    std::string quote_asset;
    bool status;
    std::chrono::system_clock::time_point updated_at;

    MarketSpecification() : status(false), updated_at(std::chrono::system_clock::now()) {}

    virtual ~MarketSpecification() = default;

    virtual json to_json() const {
        json j;
        j["symbol"] = symbol;
        if (price_precision) j["price_precision"] = *price_precision;
        if (amount_precision) j["amount_precision"] = *amount_precision;
        if (min_amount) j["min_amount"] = *min_amount;
        if (min_notional) j["min_notional"] = *min_notional;
        j["base_asset"] = base_asset;
        j["quote_asset"] = quote_asset;
        j["status"] = status;

        auto time_t = std::chrono::system_clock::to_time_t(updated_at);
        std::tm* tm = std::gmtime(&time_t);
        char buffer[32];
        std::strftime(buffer, sizeof(buffer), "%Y-%m-%dT%H:%M:%SZ", tm);
        j["updated_at"] = std::string(buffer);

        return j;
    }

    virtual void from_json(const json& j) {
        symbol = j.value("symbol", "");
        if (j.contains("price_precision") && !j["price_precision"].is_null()) {
            price_precision = j["price_precision"];
        }
        if (j.contains("amount_precision") && !j["amount_precision"].is_null()) {
            amount_precision = j["amount_precision"];
        }
        if (j.contains("min_amount") && !j["min_amount"].is_null()) {
            min_amount = j["min_amount"];
        }
        if (j.contains("min_notional") && !j["min_notional"].is_null()) {
            min_notional = j["min_notional"];
        }
        base_asset = j.value("base_asset", "");
        quote_asset = j.value("quote_asset", "");
        status = j.value("status", false);

        // Parse updated_at if present
        if (j.contains("updated_at")) {
            // Simple ISO string parsing - you might want to use a proper datetime library
            updated_at = std::chrono::system_clock::now();
        }
    }
};

struct SpotMarketSpec : public MarketSpecification {
    std::optional<double> max_order_qty;
    std::optional<double> max_price;
    std::optional<double> min_price;
    std::optional<double> tick_size;
    std::optional<double> step_size;
    bool is_margin_trading_allowed = false;

    json to_json() const override {
        json j = MarketSpecification::to_json();
        if (max_order_qty) j["max_order_qty"] = *max_order_qty;
        if (max_price) j["max_price"] = *max_price;
        if (min_price) j["min_price"] = *min_price;
        if (tick_size) j["tick_size"] = *tick_size;
        if (step_size) j["step_size"] = *step_size;
        j["is_margin_trading_allowed"] = is_margin_trading_allowed;
        return j;
    }

    void from_json(const json& j) override {
        MarketSpecification::from_json(j);
        if (j.contains("max_order_qty") && !j["max_order_qty"].is_null()) {
            max_order_qty = j["max_order_qty"];
        }
        if (j.contains("max_price") && !j["max_price"].is_null()) {
            max_price = j["max_price"];
        }
        if (j.contains("min_price") && !j["min_price"].is_null()) {
            min_price = j["min_price"];
        }
        if (j.contains("tick_size") && !j["tick_size"].is_null()) {
            tick_size = j["tick_size"];
        }
        if (j.contains("step_size") && !j["step_size"].is_null()) {
            step_size = j["step_size"];
        }
        is_margin_trading_allowed = j.value("is_margin_trading_allowed", false);
    }
};

struct FuturesMarketSpec : public MarketSpecification {
    std::string contract_type;
    std::optional<std::chrono::system_clock::time_point> delivery_date;
    std::optional<std::chrono::system_clock::time_point> onboard_date;
    std::unordered_map<std::string, json> leverage_filter;
    std::unordered_map<std::string, json> price_filter;
    std::unordered_map<std::string, json> lot_size_filter;
    double max_leverage = 20.0;
    std::optional<double> maintenance_margin_rate;
    std::optional<std::string> settlement_currency;

    json to_json() const override {
        json j = MarketSpecification::to_json();
        j["contract_type"] = contract_type;

        if (delivery_date) {
            auto time_t = std::chrono::system_clock::to_time_t(*delivery_date);
            std::tm* tm = std::gmtime(&time_t);
            char buffer[32];
            std::strftime(buffer, sizeof(buffer), "%Y-%m-%dT%H:%M:%SZ", tm);
            j["delivery_date"] = std::string(buffer);
        }

        if (onboard_date) {
            auto time_t = std::chrono::system_clock::to_time_t(*onboard_date);
            std::tm* tm = std::gmtime(&time_t);
            char buffer[32];
            std::strftime(buffer, sizeof(buffer), "%Y-%m-%dT%H:%M:%SZ", tm);
            j["onboard_date"] = std::string(buffer);
        }

        j["leverage_filter"] = json(leverage_filter);
        j["price_filter"] = json(price_filter);
        j["lot_size_filter"] = json(lot_size_filter);
        j["max_leverage"] = max_leverage;

        if (maintenance_margin_rate) j["maintenance_margin_rate"] = *maintenance_margin_rate;
        if (settlement_currency) j["settlement_currency"] = *settlement_currency;

        return j;
    }

    void from_json(const json& j) override {
        MarketSpecification::from_json(j);
        contract_type = j.value("contract_type", "");

        // Parse dates - simplified parsing
        if (j.contains("delivery_date") && !j["delivery_date"].is_null()) {
            delivery_date = std::chrono::system_clock::now();  // Placeholder
        }
        if (j.contains("onboard_date") && !j["onboard_date"].is_null()) {
            onboard_date = std::chrono::system_clock::now();  // Placeholder
        }

        if (j.contains("leverage_filter")) {
            for (auto& [key, value] : j["leverage_filter"].items()) {
                leverage_filter[key] = value;
            }
        }
        if (j.contains("price_filter")) {
            for (auto& [key, value] : j["price_filter"].items()) {
                price_filter[key] = value;
            }
        }
        if (j.contains("lot_size_filter")) {
            for (auto& [key, value] : j["lot_size_filter"].items()) {
                lot_size_filter[key] = value;
            }
        }

        max_leverage = j.value("max_leverage", 20.0);

        if (j.contains("maintenance_margin_rate") && !j["maintenance_margin_rate"].is_null()) {
            maintenance_margin_rate = j["maintenance_margin_rate"];
        }
        if (j.contains("settlement_currency") && !j["settlement_currency"].is_null()) {
            settlement_currency = j["settlement_currency"];
        }
    }
};

enum class MarketType { SPOT, FUTURES_USDT, FUTURES_COIN, MARGIN };

// Response wrapper
template <typename T>
struct MarketDataResponse {
    bool success;
    std::string error_message;
    std::optional<T> data;
    bool from_cache;

    MarketDataResponse() : success(false), from_cache(false) {}

    static MarketDataResponse<T> success_response(const T& data, bool cached = false) {
        MarketDataResponse<T> response;
        response.success = true;
        response.data = data;
        response.from_cache = cached;
        return response;
    }

    static MarketDataResponse<T> error_response(const std::string& error) {
        MarketDataResponse<T> response;
        response.success = false;
        response.error_message = error;
        return response;
    }
};
