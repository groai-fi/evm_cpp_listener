#include "market_data_service.h"

#include <algorithm>
#include <iostream>

MarketDataService::MarketDataService(const string& redis_url, chrono::seconds cache_ttl)
    : market_data_key_prefix_("market_data") {
    redis_client_ = make_unique<RedisKVClient>(redis_url, cache_ttl);
}

bool MarketDataService::initialize() {
    if (!redis_client_->connect()) {
        cerr << "Failed to connect to Redis" << endl;
        return false;
    }

    cout << "MarketDataService initialized successfully" << endl;
    return true;
}

bool MarketDataService::is_connected() const { return redis_client_->is_connected(); }

string MarketDataService::build_market_data_key(const string& exchange,
                                                const string& market_type) const {
    return market_data_key_prefix_ + ":" + exchange + ":" + market_type;
}

MarketType MarketDataService::parse_market_type(const string& market_type_str) const {
    string lower_market_type = market_type_str;
    transform(lower_market_type.begin(), lower_market_type.end(), lower_market_type.begin(),
              ::tolower);

    if (lower_market_type == "spot") {
        return MarketType::SPOT;
    } else if (lower_market_type == "futures_usdt") {
        return MarketType::FUTURES_USDT;
    } else {
        throw invalid_argument("Unknown market type: " + market_type_str);
    }
}

MarketSpecVariant MarketDataService::create_market_spec_from_json(const json& market_json,
                                                                  MarketType market_type) const {
    if (market_type == MarketType::SPOT) {
        SpotMarketSpec spec;
        spec.from_json(market_json);
        return spec;
    } else {
        FuturesMarketSpec spec;
        spec.from_json(market_json);
        return spec;
    }
}

MarketDataResponse<MarketSpecVariant> MarketDataService::get_market_data(const string& exchange,
                                                                         const string& market_type,
                                                                         const string& symbol) {
    try {
        // Build Redis key
        string redis_key = build_market_data_key(exchange, market_type);

        // Get market data from Redis (with cache)
        auto market_data_opt = redis_client_->get_json(redis_key, true);
        if (!market_data_opt.has_value()) {
            return MarketDataResponse<MarketSpecVariant>::error_response(
                "Market data not found for " + exchange + ":" + market_type);
        }

        const json& market_data = *market_data_opt;

        // Validate market data structure
        if (!market_data.contains("markets")) {
            return MarketDataResponse<MarketSpecVariant>::error_response(
                "Invalid market data format: missing 'markets' field");
        }

        const json& markets = market_data["markets"];

        // Find the specific symbol
        if (!markets.contains(symbol)) {
            return MarketDataResponse<MarketSpecVariant>::error_response(
                "Symbol '" + symbol + "' not found in " + exchange + ":" + market_type);
        }

        const json& market_json = markets[symbol];

        // Parse market type and create appropriate spec
        MarketType mt = parse_market_type(market_type);
        MarketSpecVariant market_spec = create_market_spec_from_json(market_json, mt);

        return MarketDataResponse<MarketSpecVariant>::success_response(market_spec, true);

    } catch (const exception& e) {
        return MarketDataResponse<MarketSpecVariant>::error_response(
            "Error retrieving market data: " + string(e.what()));
    }
}

MarketDataResponse<json> MarketDataService::get_all_market_data(const string& exchange,
                                                                const string& market_type) {
    try {
        string redis_key = build_market_data_key(exchange, market_type);

        auto market_data_opt = redis_client_->get_json(redis_key, true);
        if (!market_data_opt.has_value()) {
            return MarketDataResponse<json>::error_response("Market data not found for " +
                                                            exchange + ":" + market_type);
        }

        return MarketDataResponse<json>::success_response(*market_data_opt, true);

    } catch (const exception& e) {
        return MarketDataResponse<json>::error_response("Error retrieving all market data: " +
                                                        string(e.what()));
    }
}

void MarketDataService::clear_cache() { redis_client_->clear_cache(); }

size_t MarketDataService::cache_size() const { return redis_client_->cache_size(); }
