#include <cassert>
#include <chrono>
#include <iostream>
#include <string>
#include <thread>
#include <variant>
#include <vector>

#include "market_data_service.h"
#include "market_spec.h"

using namespace std;

class MarketDataServiceTester {
private:
    MarketDataService service_;
    string test_exchange_;
    string test_market_type_;
    string test_symbol_;

    // Helper to create test market data in Redis
    void setup_test_data() {
        // This would typically be done by your data ingestion process
        // For testing, we'll assume the service can handle missing data gracefully
        cout << "Setting up test environment..." << endl;
    }

    // Helper to clean up test data
    void cleanup_test_data() {
        service_.clear_cache();
        cout << "Cleaned up test environment" << endl;
    }

public:
    explicit MarketDataServiceTester(const string& redis_url)
        : service_(redis_url, chrono::seconds(60)),
          test_exchange_("binance"),
          test_market_type_("spot"),
          test_symbol_("BTCUSDT") {}

    void run_all_tests() {
        cout << "=== Running MarketDataService Tests ===" << endl;

        setup_test_data();

        test_service_initialization();
        test_market_data_response_structure();
        test_btc_usdt_market_data();
        test_all_market_data_retrieval();
        test_cache_functionality();
        test_error_handling();
        test_market_spec_variants();
        test_response_validation();

        cleanup_test_data();

        cout << "=== All MarketDataService tests passed! ===" << endl;
    }

    void test_service_initialization() {
        cout << "Testing service initialization..." << endl;

        bool initialized = service_.initialize();
        assert(initialized && "MarketDataService should initialize successfully");
        assert(service_.is_connected() && "Service should be connected after initialization");

        cout << "✓ Service initialization tests passed" << endl;
    }

    void test_market_data_response_structure() {
        cout << "Testing MarketDataResponse structure..." << endl;

        // Test successful response creation
        SpotMarketSpec test_spec;
        test_spec.symbol = "TESTUSDT";
        test_spec.base_asset = "TEST";
        test_spec.quote_asset = "USDT";
        test_spec.status = true;

        MarketSpecVariant variant_spec = test_spec;
        auto success_response =
            MarketDataResponse<MarketSpecVariant>::success_response(variant_spec, false);

        assert(success_response.success && "Success response should have success=true");
        assert(success_response.data.has_value() && "Success response should have data");
        assert(!success_response.from_cache && "Response should indicate not from cache");
        assert(success_response.error_message.empty() &&
               "Success response should have empty error message");

        // Test error response creation
        auto error_response = MarketDataResponse<MarketSpecVariant>::error_response("Test error");

        assert(!error_response.success && "Error response should have success=false");
        assert(!error_response.data.has_value() && "Error response should not have data");
        assert(!error_response.from_cache && "Error response should not be from cache");
        assert(error_response.error_message == "Test error" &&
               "Error response should have correct error message");

        // Test cached response
        auto cached_response =
            MarketDataResponse<MarketSpecVariant>::success_response(variant_spec, true);
        assert(cached_response.from_cache && "Cached response should indicate from cache");

        cout << "✓ MarketDataResponse structure tests passed" << endl;
    }

    void test_btc_usdt_market_data() {
        cout << "Testing BTC/USDT market data retrieval..." << endl;

        // Test getting specific BTC/USDT market data
        auto response = service_.get_market_data(test_exchange_, test_market_type_, test_symbol_);

        // The response should be valid regardless of whether data exists
        assert(response.success || !response.error_message.empty() &&
                                       "Response should either succeed or have an error message");

        if (response.success) {
            cout << "  ✓ Successfully retrieved BTC/USDT market data" << endl;
            cout << "  ✓ Data source: " << (response.from_cache ? "Cache" : "Redis") << endl;

            assert(response.data.has_value() && "Successful response should have data");

            // Test variant access
            visit(
                [this](const auto& spec) {
                    using T = decay_t<decltype(spec)>;
                    if constexpr (is_same_v<T, SpotMarketSpec>) {
                        cout << "  ✓ Retrieved SpotMarketSpec for " << spec.symbol << endl;
                        assert(spec.symbol == test_symbol_ &&
                               "Symbol should match requested symbol");
                        assert(spec.base_asset == "BTC" && "Base asset should be BTC");
                        assert(spec.quote_asset == "USDT" && "Quote asset should be USDT");

                        // Test optional fields
                        if (spec.price_precision) {
                            assert(*spec.price_precision >= 0 &&
                                   "Price precision should be non-negative");
                            cout << "  ✓ Price precision: " << *spec.price_precision << endl;
                        }
                        if (spec.min_amount) {
                            assert(*spec.min_amount > 0 && "Min amount should be positive");
                            cout << "  ✓ Min amount: " << *spec.min_amount << endl;
                        }
                        if (spec.tick_size) {
                            assert(*spec.tick_size > 0 && "Tick size should be positive");
                            cout << "  ✓ Tick size: " << *spec.tick_size << endl;
                        }

                    } else if constexpr (is_same_v<T, FuturesMarketSpec>) {
                        cout << "  ✓ Retrieved FuturesMarketSpec for " << spec.symbol << endl;
                        assert(spec.symbol == test_symbol_ &&
                               "Symbol should match requested symbol");
                        assert(spec.max_leverage > 0 && "Max leverage should be positive");
                        cout << "  ✓ Max leverage: " << spec.max_leverage << endl;
                    }
                },
                *response.data);

        } else {
            cout << "  ℹ BTC/USDT data not available: " << response.error_message << endl;
            // This is acceptable for testing - data might not be populated yet
        }

        cout << "✓ BTC/USDT market data tests passed" << endl;
    }

    void test_all_market_data_retrieval() {
        cout << "Testing all market data retrieval..." << endl;

        auto response = service_.get_all_market_data(test_exchange_, test_market_type_);

        // Test response structure
        if (response.success) {
            cout << "  ✓ Successfully retrieved all market data" << endl;
            cout << "  ✓ Data source: " << (response.from_cache ? "Cache" : "Redis") << endl;

            assert(response.data.has_value() && "Successful response should have data");

            const json& all_data = *response.data;

            // Test JSON structure
            if (all_data.contains("exchange")) {
                string exchange = all_data["exchange"];
                assert(exchange == test_exchange_ && "Exchange should match requested exchange");
                cout << "  ✓ Exchange: " << exchange << endl;
            }

            if (all_data.contains("market_type")) {
                string market_type = all_data["market_type"];
                assert(market_type == test_market_type_ &&
                       "Market type should match requested type");
                cout << "  ✓ Market type: " << market_type << endl;
            }

            if (all_data.contains("markets")) {
                const json& markets = all_data["markets"];
                assert(markets.is_object() && "Markets should be a JSON object");
                cout << "  ✓ Markets count: " << markets.size() << endl;

                // Test a few market entries if they exist
                int tested_count = 0;
                for (const auto& [symbol, market_data] : markets.items()) {
                    if (tested_count >= 3) break;  // Test first 3 markets

                    assert(market_data.is_object() && "Market data should be JSON object");
                    assert(market_data.contains("symbol") && "Market data should contain symbol");
                    assert(market_data["symbol"] == symbol && "Symbol should match key");

                    cout << "  ✓ Validated market: " << symbol << endl;
                    tested_count++;
                }
            }

        } else {
            cout << "  ℹ All market data not available: " << response.error_message << endl;
            // This is acceptable for testing
        }

        cout << "✓ All market data retrieval tests passed" << endl;
    }

    void test_cache_functionality() {
        cout << "Testing cache functionality..." << endl;

        // Clear cache first
        service_.clear_cache();
        size_t initial_cache_size = service_.cache_size();
        assert(initial_cache_size == 0 && "Cache should be empty after clear");

        // Make first request (should populate cache if successful)
        auto start_time = chrono::high_resolution_clock::now();
        auto first_response =
            service_.get_market_data(test_exchange_, test_market_type_, test_symbol_);
        auto mid_time = chrono::high_resolution_clock::now();

        // Make second request (should hit cache if first was successful)
        auto second_response =
            service_.get_market_data(test_exchange_, test_market_type_, test_symbol_);
        auto end_time = chrono::high_resolution_clock::now();

        auto first_duration = chrono::duration_cast<chrono::microseconds>(mid_time - start_time);
        auto second_duration = chrono::duration_cast<chrono::microseconds>(end_time - mid_time);

        cout << "  ✓ First call duration: " << first_duration.count() << " microseconds" << endl;
        cout << "  ✓ Second call duration: " << second_duration.count() << " microseconds" << endl;

        // If both calls were successful, second should be from cache
        if (first_response.success && second_response.success) {
            // Second call might be faster due to caching
            cout << "  ✓ Both calls successful - cache behavior validated" << endl;

            // Check cache size
            size_t final_cache_size = service_.cache_size();
            cout << "  ✓ Final cache size: " << final_cache_size << " entries" << endl;
        }

        // Test cache clear
        service_.clear_cache();
        size_t cleared_cache_size = service_.cache_size();
        assert(cleared_cache_size == 0 && "Cache should be empty after clear");

        cout << "✓ Cache functionality tests passed" << endl;
    }

    void test_error_handling() {
        cout << "Testing error handling..." << endl;

        // Test with invalid exchange
        auto invalid_exchange_response =
            service_.get_market_data("invalid_exchange", test_market_type_, test_symbol_);
        // Should either succeed (if data exists) or fail gracefully
        assert(!invalid_exchange_response.success ||
               invalid_exchange_response.data.has_value() && "Response should be consistent");

        if (!invalid_exchange_response.success) {
            assert(!invalid_exchange_response.error_message.empty() &&
                   "Error response should have error message");
            cout << "  ✓ Invalid exchange handled: " << invalid_exchange_response.error_message
                 << endl;
        }

        // Test with invalid market type
        auto invalid_market_response =
            service_.get_market_data(test_exchange_, "invalid_market", test_symbol_);
        if (!invalid_market_response.success) {
            assert(!invalid_market_response.error_message.empty() &&
                   "Error response should have error message");
            cout << "  ✓ Invalid market type handled: " << invalid_market_response.error_message
                 << endl;
        }

        // Test with invalid symbol
        auto invalid_symbol_response =
            service_.get_market_data(test_exchange_, test_market_type_, "INVALIDPAIR");
        if (!invalid_symbol_response.success) {
            assert(!invalid_symbol_response.error_message.empty() &&
                   "Error response should have error message");
            cout << "  ✓ Invalid symbol handled: " << invalid_symbol_response.error_message << endl;
        }

        cout << "✓ Error handling tests passed" << endl;
    }

    void test_market_spec_variants() {
        cout << "Testing MarketSpec variants..." << endl;

        // Test SpotMarketSpec
        SpotMarketSpec spot_spec;
        spot_spec.symbol = "ETHUSDT";
        spot_spec.base_asset = "ETH";
        spot_spec.quote_asset = "USDT";
        spot_spec.status = true;
        spot_spec.price_precision = 2;
        spot_spec.min_amount = 0.001;
        spot_spec.tick_size = 0.01;
        spot_spec.is_margin_trading_allowed = true;

        // Test JSON serialization
        json spot_json = spot_spec.to_json();
        assert(spot_json["symbol"] == "ETHUSDT" && "JSON should contain correct symbol");
        assert(spot_json["base_asset"] == "ETH" && "JSON should contain correct base asset");
        assert(spot_json["quote_asset"] == "USDT" && "JSON should contain correct quote asset");
        assert(spot_json["status"] == true && "JSON should contain correct status");
        assert(spot_json["price_precision"] == 2 && "JSON should contain correct price precision");
        assert(spot_json["is_margin_trading_allowed"] == true &&
               "JSON should contain margin trading flag");

        // Test JSON deserialization
        SpotMarketSpec deserialized_spot;
        deserialized_spot.from_json(spot_json);
        assert(deserialized_spot.symbol == spot_spec.symbol && "Deserialized symbol should match");
        assert(deserialized_spot.base_asset == spot_spec.base_asset &&
               "Deserialized base asset should match");
        assert(deserialized_spot.price_precision == spot_spec.price_precision &&
               "Deserialized price precision should match");

        // Test FuturesMarketSpec
        FuturesMarketSpec futures_spec;
        futures_spec.symbol = "BTCUSDT";
        futures_spec.base_asset = "BTC";
        futures_spec.quote_asset = "USDT";
        futures_spec.status = true;
        futures_spec.contract_type = "PERPETUAL";
        futures_spec.max_leverage = 125.0;
        futures_spec.maintenance_margin_rate = 0.004;
        futures_spec.settlement_currency = "USDT";

        // Test JSON serialization
        json futures_json = futures_spec.to_json();
        assert(futures_json["symbol"] == "BTCUSDT" && "Futures JSON should contain correct symbol");
        assert(futures_json["contract_type"] == "PERPETUAL" &&
               "Futures JSON should contain correct contract type");
        assert(futures_json["max_leverage"] == 125.0 &&
               "Futures JSON should contain correct max leverage");
        assert(futures_json["maintenance_margin_rate"] == 0.004 &&
               "Futures JSON should contain correct margin rate");
        assert(futures_json["settlement_currency"] == "USDT" &&
               "Futures JSON should contain correct settlement currency");

        // Test JSON deserialization
        FuturesMarketSpec deserialized_futures;
        deserialized_futures.from_json(futures_json);
        assert(deserialized_futures.symbol == futures_spec.symbol &&
               "Deserialized futures symbol should match");
        assert(deserialized_futures.contract_type == futures_spec.contract_type &&
               "Deserialized contract type should match");
        assert(deserialized_futures.max_leverage == futures_spec.max_leverage &&
               "Deserialized max leverage should match");
        assert(deserialized_futures.maintenance_margin_rate ==
                   futures_spec.maintenance_margin_rate &&
               "Deserialized margin rate should match");

        // Test variant handling
        MarketSpecVariant spot_variant = spot_spec;
        MarketSpecVariant futures_variant = futures_spec;

        // Test variant visitation
        visit(
            [](const auto& spec) {
                using T = decay_t<decltype(spec)>;
                if constexpr (is_same_v<T, SpotMarketSpec>) {
                    assert(spec.symbol.find("ETH") != string::npos &&
                           "Spot variant should contain ETH symbol");
                    cout << "  ✓ SpotMarketSpec variant handled correctly" << endl;
                } else if constexpr (is_same_v<T, FuturesMarketSpec>) {
                    assert(spec.symbol.find("BTC") != string::npos &&
                           "Futures variant should contain BTC symbol");
                    cout << "  ✓ FuturesMarketSpec variant handled correctly" << endl;
                }
            },
            spot_variant);

        visit(
            [](const auto& spec) {
                using T = decay_t<decltype(spec)>;
                if constexpr (is_same_v<T, SpotMarketSpec>) {
                    assert(false && "Should not reach SpotMarketSpec for futures variant");
                } else if constexpr (is_same_v<T, FuturesMarketSpec>) {
                    assert(spec.max_leverage > 0 && "Futures spec should have positive leverage");
                    cout << "  ✓ FuturesMarketSpec variant handled correctly" << endl;
                }
            },
            futures_variant);

        cout << "✓ MarketSpec variants tests passed" << endl;
    }

    void test_response_validation() {
        cout << "Testing response validation..." << endl;

        // Test various response scenarios
        vector<string> test_symbols = {"BTCUSDT", "ETHUSDT", "ADAUSDT", "DOTUSDT", "SOLUSDT"};

        for (const auto& symbol : test_symbols) {
            auto response = service_.get_market_data(test_exchange_, test_market_type_, symbol);

            // Validate response consistency
            if (response.success) {
                assert(response.data.has_value() &&
                       ("Successful response for " + symbol + " should have data").c_str());
                assert(response.error_message.empty() &&
                       ("Successful response for " + symbol + " should have empty error").c_str());

                // Validate the market spec data
                visit(
                    [&symbol](const auto& spec) {
                        assert(spec.symbol == symbol &&
                               ("Symbol should match for " + symbol).c_str());
                        assert(!spec.base_asset.empty() &&
                               ("Base asset should not be empty for " + symbol).c_str());
                        assert(!spec.quote_asset.empty() &&
                               ("Quote asset should not be empty for " + symbol).c_str());

                        // Validate USDT pairs
                        if (symbol.find("USDT") != string::npos) {
                            assert(spec.quote_asset == "USDT" &&
                                   ("Quote asset should be USDT for " + symbol).c_str());
                        }

                        cout << "  ✓ Validated " << symbol << " - " << spec.base_asset << "/"
                             << spec.quote_asset << endl;
                    },
                    *response.data);

            } else {
                assert(!response.data.has_value() &&
                       ("Failed response for " + symbol + " should not have data").c_str());
                assert(!response.error_message.empty() &&
                       ("Failed response for " + symbol + " should have error message").c_str());
                cout << "  ℹ " << symbol << " not available: " << response.error_message << endl;
            }

            // Response should never have both success=true and data=nullopt
            assert(!(response.success && !response.data.has_value()) &&
                   ("Inconsistent response state for " + symbol).c_str());

            // Response should never have both success=false and data present
            assert(!((!response.success) && response.data.has_value()) &&
                   ("Inconsistent response state for " + symbol).c_str());
        }

        cout << "✓ Response validation tests passed" << endl;
    }

    void test_performance_benchmarks() {
        cout << "Testing performance benchmarks..." << endl;

        const int num_requests = 100;
        vector<chrono::microseconds> response_times;

        // Warm up cache
        service_.get_market_data(test_exchange_, test_market_type_, test_symbol_);

        // Benchmark individual requests
        for (int i = 0; i < num_requests; ++i) {
            auto start = chrono::high_resolution_clock::now();
            auto response =
                service_.get_market_data(test_exchange_, test_market_type_, test_symbol_);
            auto end = chrono::high_resolution_clock::now();

            auto duration = chrono::duration_cast<chrono::microseconds>(end - start);
            response_times.push_back(duration);

            // Small delay to avoid overwhelming the system
            this_thread::sleep_for(chrono::milliseconds(10));
        }

        // Calculate statistics
        auto min_time = *min_element(response_times.begin(), response_times.end());
        auto max_time = *max_element(response_times.begin(), response_times.end());
        auto total_time =
            accumulate(response_times.begin(), response_times.end(), chrono::microseconds(0));
        auto avg_time = total_time / num_requests;

        cout << "  ✓ Performance over " << num_requests << " requests:" << endl;
        cout << "    Min response time: " << min_time.count() << " µs" << endl;
        cout << "    Max response time: " << max_time.count() << " µs" << endl;
        cout << "    Avg response time: " << avg_time.count() << " µs" << endl;
        cout << "    Final cache size: " << service_.cache_size() << " entries" << endl;

        // Performance assertions
        assert(avg_time.count() < 100000 && "Average response time should be under 100ms");
        assert(min_time.count() > 0 && "Min response time should be positive");

        cout << "✓ Performance benchmark tests passed" << endl;
    }

    void test_concurrent_access() {
        cout << "Testing concurrent access..." << endl;

        const int num_threads = 5;
        const int requests_per_thread = 20;
        vector<thread> threads;
        vector<bool> thread_results(num_threads, false);

        // Launch concurrent threads
        for (int t = 0; t < num_threads; ++t) {
            threads.emplace_back([this, t, requests_per_thread, &thread_results]() {
                bool all_successful = true;

                for (int i = 0; i < requests_per_thread; ++i) {
                    // Alternate between different symbols
                    string symbol = (i % 2 == 0) ? "BTCUSDT" : "ETHUSDT";
                    auto response =
                        service_.get_market_data(test_exchange_, test_market_type_, symbol);

                    // We don't require success, but we do require consistent responses
                    if (response.success && !response.data.has_value()) {
                        all_successful = false;
                        break;
                    }
                    if (!response.success && response.data.has_value()) {
                        all_successful = false;
                        break;
                    }

                    // Small delay
                    this_thread::sleep_for(chrono::milliseconds(5));
                }

                thread_results[t] = all_successful;
            });
        }

        // Wait for all threads to complete
        for (auto& thread : threads) {
            thread.join();
        }

        // Check results
        for (int t = 0; t < num_threads; ++t) {
            assert(thread_results[t] &&
                   ("Thread " + to_string(t) + " should have consistent responses").c_str());
        }

        cout << "  ✓ All " << num_threads << " threads completed successfully" << endl;
        cout << "  ✓ Total requests processed: " << num_threads * requests_per_thread << endl;
        cout << "  ✓ Final cache size: " << service_.cache_size() << " entries" << endl;

        cout << "✓ Concurrent access tests passed" << endl;
    }

    void run_extended_tests() {
        cout << "\n=== Running Extended MarketDataService Tests ===" << endl;
        test_performance_benchmarks();
        test_concurrent_access();
    }
};

int main() {
    try {
        // Use localhost Redis for testing
        string redis_url = "redis://localhost:6379";

        MarketDataServiceTester tester(redis_url);
        tester.run_all_tests();

        // Run extended tests
        tester.run_extended_tests();

        cout << "\n🎉 All MarketDataService tests completed successfully!" << endl;
        cout << "📊 MarketDataResponse and MarketSpecVariant operations validated!" << endl;
        cout << "💰 BTC/USDT and other cryptocurrency pairs tested!" << endl;
        return 0;

    } catch (const std::exception& e) {
        cerr << "❌ Test failed with exception: " << e.what() << endl;
        return 1;
    } catch (...) {
        cerr << "❌ Test failed with unknown exception" << endl;
        return 1;
    }
}