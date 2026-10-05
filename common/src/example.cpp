#include <market_data_service.h>

#include <iostream>
#include <variant>

using namespace std;

void print_spot_market_spec(const SpotMarketSpec& spec) {
    cout << "=== Spot Market Specification ===" << endl;
    cout << "Symbol: " << spec.symbol << endl;
    cout << "Base Asset: " << spec.base_asset << endl;
    cout << "Quote Asset: " << spec.quote_asset << endl;
    cout << "Status: " << (spec.status ? "Active" : "Inactive") << endl;

    if (spec.price_precision) {
        cout << "Price Precision: " << *spec.price_precision << endl;
    }
    if (spec.min_amount) {
        cout << "Min Amount: " << *spec.min_amount << endl;
    }
    if (spec.max_order_qty) {
        cout << "Max Order Qty: " << *spec.max_order_qty << endl;
    }
    if (spec.tick_size) {
        cout << "Tick Size: " << *spec.tick_size << endl;
    }

    cout << "Margin Trading Allowed: " << (spec.is_margin_trading_allowed ? "Yes" : "No") << endl;
}

void print_futures_market_spec(const FuturesMarketSpec& spec) {
    cout << "=== Futures Market Specification ===" << endl;
    cout << "Symbol: " << spec.symbol << endl;
    cout << "Base Asset: " << spec.base_asset << endl;
    cout << "Quote Asset: " << spec.quote_asset << endl;
    cout << "Contract Type: " << spec.contract_type << endl;
    cout << "Status: " << (spec.status ? "Active" : "Inactive") << endl;
    cout << "Max Leverage: " << spec.max_leverage << endl;

    if (spec.maintenance_margin_rate) {
        cout << "Maintenance Margin Rate: " << *spec.maintenance_margin_rate << endl;
    }
    if (spec.settlement_currency) {
        cout << "Settlement Currency: " << *spec.settlement_currency << endl;
    }
}

int main() {
    // Initialize market data service
    string redis_url = "redis://localhost:6379";
    MarketDataService service(redis_url);

    if (!service.initialize()) {
        cerr << "Failed to initialize MarketDataService" << endl;
        return 1;
    }

    cout << "MarketDataService initialized successfully!" << endl;

    // Example 1: Get specific market data
    cout << "\n=== Getting Specific Market Data ===" << endl;
    auto response = service.get_market_data("binance", "spot", "BTCUSDT");

    if (response.success) {
        cout << "Successfully retrieved market data for BTCUSDT" << endl;
        cout << "Data source: " << (response.from_cache ? "Cache" : "Redis") << endl;

        // Visit the variant to print the appropriate spec
        visit(
            [](const auto& spec) {
                using T = decay_t<decltype(spec)>;
                if constexpr (is_same_v<T, SpotMarketSpec>) {
                    print_spot_market_spec(spec);
                } else if constexpr (is_same_v<T, FuturesMarketSpec>) {
                    print_futures_market_spec(spec);
                }
            },
            *response.data);
    } else {
        cout << "Failed to get market data: " << response.error_message << endl;
    }

    // Example 2: Get all market data
    cout << "\n=== Getting All Market Data ===" << endl;
    auto all_data_response = service.get_all_market_data("binance", "spot");

    if (all_data_response.success) {
        const json& all_data = *all_data_response.data;
        cout << "Retrieved all market data successfully" << endl;
        cout << "Exchange: " << all_data.value("exchange", "unknown") << endl;
        cout << "Market Type: " << all_data.value("market_type", "unknown") << endl;

        if (all_data.contains("markets")) {
            const json& markets = all_data["markets"];
            cout << "Total markets: " << markets.size() << endl;

            // Print first few market symbols
            cout << "Sample markets: ";
            int count = 0;
            for (const auto& [symbol, spec] : markets.items()) {
                if (count >= 5) break;
                cout << symbol << " ";
                count++;
            }
            cout << endl;
        }
    } else {
        cout << "Failed to get all market data: " << all_data_response.error_message << endl;
    }

    // Example 3: Test cache performance
    cout << "\n=== Testing Cache Performance ===" << endl;
    auto start = chrono::high_resolution_clock::now();

    // First call (should hit Redis)
    auto first_call = service.get_market_data("binance", "spot", "ETHUSDT");
    auto mid = chrono::high_resolution_clock::now();

    // Second call (should hit cache)
    auto second_call = service.get_market_data("binance", "spot", "ETHUSDT");
    auto end = chrono::high_resolution_clock::now();

    auto first_duration = chrono::duration_cast<chrono::microseconds>(mid - start);
    auto second_duration = chrono::duration_cast<chrono::microseconds>(end - mid);

    cout << "First call (Redis): " << first_duration.count() << " microseconds" << endl;
    cout << "Second call (Cache): " << second_duration.count() << " microseconds" << endl;
    cout << "Cache size: " << service.cache_size() << " entries" << endl;

    return 0;
}
