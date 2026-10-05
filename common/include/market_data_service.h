#pragma once

#include <memory>
#include <variant>

#include "market_spec.h"
#include "redis_kv_client.h"

using MarketSpecVariant = variant<SpotMarketSpec, FuturesMarketSpec>;
using namespace std;

class MarketDataService {
private:
    unique_ptr<RedisKVClient> redis_client_;
    string market_data_key_prefix_;

    // Helper methods
    string build_market_data_key(const string& exchange, const string& market_type) const;
    MarketType parse_market_type(const string& market_type_str) const;
    MarketSpecVariant create_market_spec_from_json(const json& market_json,
                                                   MarketType market_type) const;

public:
    explicit MarketDataService(const string& redis_url,
                               chrono::seconds cache_ttl = chrono::seconds(300));
    ~MarketDataService() = default;

    // Disable copy constructor and assignment
    MarketDataService(const MarketDataService&) = delete;
    MarketDataService& operator=(const MarketDataService&) = delete;

    bool initialize();
    bool is_connected() const;

    // Get market data for a specific symbol
    MarketDataResponse<MarketSpecVariant> get_market_data(const string& exchange,
                                                          const string& market_type,
                                                          const string& symbol);

    // Get all market data for an exchange and market type
    MarketDataResponse<json> get_all_market_data(const string& exchange, const string& market_type);

    // Cache management
    void clear_cache();
    size_t cache_size() const;
};
