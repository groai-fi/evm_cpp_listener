#pragma once

#include <uuid/uuid.h>

#include <chrono>
#include <iostream>
#include <random>
#include <string>

using namespace std;

// USAGE
// LOG_ERROR("Error parsing position response for " << symbol << ": " << e.what());
#define LOG_ERROR(msg) cerr << "❌ [" << __FILE__ << ":" << __LINE__ << "] " << msg << endl
#define LOG_INFO(msg) cerr << "ℹ️ [" << __FILE__ << ":" << __LINE__ << "] " << msg << endl
#define LOG_DEBUG(msg) cerr << "🔍 [" << __FILE__ << ":" << __LINE__ << "] " << msg << endl

inline int64_t getCurrentTimestampMs() {
    return chrono::duration_cast<chrono::milliseconds>(
               chrono::high_resolution_clock::now().time_since_epoch())
        .count();
}

inline bool ends_with(const string& str, const string& suffix) {
    return str.size() >= suffix.size() &&
           str.compare(str.size() - suffix.size(), suffix.size(), suffix) == 0;
}

inline string generate_request_id() {
    uuid_t uuid;
    uuid_generate_random(uuid);
    char uuid_str[37];
    uuid_unparse(uuid, uuid_str);
    return string{uuid_str};
}

inline string generate_random_string(size_t length) {
    // Define the set of characters to choose from
    const string characters = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

    // Create a random number generator engine
    // random_device provides a non-deterministic source of randomness
    // mt19937 is a Mersenne Twister pseudo-random number generator
    random_device rd;
    mt19937 generator(rd());

    // Create a uniform integer distribution to select indices from the characters string
    uniform_int_distribution<> distribution(0, characters.length() - 1);

    string random_string;
    random_string.reserve(length);  // Optimize by reserving memory

    // Generate characters and append them to the string
    for (size_t i = 0; i < length; ++i) {
        random_string += characters[distribution(generator)];
    }

    return random_string;
}

/**
 * Parse URL to extract host and port
 *
 * @param url The full URL (must start with http:// or https://)
 * @param host Output parameter for the extracted host
 * @param port Output parameter for the extracted port
 * @return true if parsing succeeded, false otherwise
 *
 * Uses standard HTTP/HTTPS port defaults:
 *   - http:// defaults to port 80
 *   - https:// defaults to port 443
 *
 * Examples:
 *   - "http://localhost" -> host="localhost", port=80
 *   - "https://example.com" -> host="example.com", port=443
 *   - "http://localhost:8080" -> host="localhost", port=8080
 *   - "http://192.168.1.1:3000/api" -> host="192.168.1.1", port=3000
 *   - "invalid-url" -> returns false (no protocol)
 */
inline bool parse_url_host_port(const string& url, string& host, int& port) {
    // 1. Check for required protocol (http:// or https://)
    size_t host_start = 0;
    int default_port = 80;  // Default for HTTP

    if (url.find("https://") == 0) {
        host_start = 8;      // Length of "https://"
        default_port = 443;  // HTTPS standard port
    } else if (url.find("http://") == 0) {
        host_start = 7;     // Length of "http://"
        default_port = 80;  // HTTP standard port
    } else {
        // Protocol is required
        cerr << "❌ URL must start with http:// or https://: " << url << endl;
        return false;
    }

    // 2. Find the end of the authority section (host:port)
    // Authority ends at the first '/' after the protocol, or at the end of string
    size_t path_start = url.find('/', host_start);
    string authority;

    if (path_start == string::npos) {
        // No path found (e.g., "http://host:8080")
        authority = url.substr(host_start);
    } else {
        // Path found (e.g., "http://host:8080/api")
        authority = url.substr(host_start, path_start - host_start);
    }

    // 3. Split authority into host and port
    size_t colon_pos = authority.find(':');

    if (colon_pos != string::npos) {
        // Port is explicitly specified
        host = authority.substr(0, colon_pos);
        string port_str = authority.substr(colon_pos + 1);

        try {
            port = stoi(port_str);
            if (port <= 0 || port > 65535) {
                cerr << "❌ Invalid port number (must be 1-65535): " << port << endl;
                return false;
            }
        } catch (...) {
            cerr << "❌ Invalid port format: " << port_str << endl;
            return false;
        }
    } else {
        // No port specified, use protocol-specific default
        host = authority;
        port = default_port;
    }

    // 4. Validate host is not empty
    if (host.empty()) {
        cerr << "❌ Host cannot be empty in URL: " << url << endl;
        return false;
    }

    cout << "🔗 Parsed URL - Host: " << host << ", Port: " << port << endl;
    return true;
}

/**
 * Parse URL to extract host, port, path, and scheme
 *
 * @param url The full URL (must start with http:// or https://)
 * @param host Output parameter for the extracted host
 * @param port Output parameter for the extracted port
 * @param path Output parameter for the extracted path (defaults to "/" if no path)
 * @param is_https Output parameter indicating if the URL uses HTTPS (true) or HTTP (false)
 * @return true if parsing succeeded, false otherwise
 *
 * Uses standard HTTP/HTTPS port defaults:
 *   - http:// defaults to port 80
 *   - https:// defaults to port 443
 *
 * Examples:
 *   - "http://localhost" -> host="localhost", port=80, path="/", is_https=false
 *   - "https://example.com/api/v1" -> host="example.com", port=443, path="/api/v1", is_https=true
 *   - "http://localhost:8080/webhook" -> host="localhost", port=8080, path="/webhook",
 * is_https=false
 *   - "https://shuttle.proxy.rlwy.net:19554/webhook-test/abc" -> host="shuttle.proxy.rlwy.net",
 * port=19554, path="/webhook-test/abc", is_https=true
 */
inline bool parse_url_host_port_path(const string& url, string& host, int& port, string& path,
                                     bool& is_https) {
    // 1. Check for required protocol (http:// or https://)
    size_t host_start = 0;
    int default_port = 80;  // Default for HTTP

    if (url.find("https://") == 0) {
        host_start = 8;      // Length of "https://"
        default_port = 443;  // HTTPS standard port
        is_https = true;
    } else if (url.find("http://") == 0) {
        host_start = 7;     // Length of "http://"
        default_port = 80;  // HTTP standard port
        is_https = false;
    } else {
        // Protocol is required
        cerr << "❌ URL must start with http:// or https://: " << url << endl;
        return false;
    }

    // 2. Find the end of the authority section (host:port)
    // Authority ends at the first '/' after the protocol, or at the end of string
    size_t path_start = url.find('/', host_start);
    string authority;

    if (path_start == string::npos) {
        // No path found (e.g., "http://host:8080")
        authority = url.substr(host_start);
        path = "/";  // Default path
    } else {
        // Path found (e.g., "http://host:8080/api")
        authority = url.substr(host_start, path_start - host_start);
        path = url.substr(path_start);  // Extract the path
    }

    // 3. Split authority into host and port
    size_t colon_pos = authority.find(':');

    if (colon_pos != string::npos) {
        // Port is explicitly specified
        host = authority.substr(0, colon_pos);
        string port_str = authority.substr(colon_pos + 1);

        try {
            port = stoi(port_str);
            if (port <= 0 || port > 65535) {
                cerr << "❌ Invalid port number (must be 1-65535): " << port << endl;
                return false;
            }
        } catch (...) {
            cerr << "❌ Invalid port format: " << port_str << endl;
            return false;
        }
    } else {
        // No port specified, use protocol-specific default
        host = authority;
        port = default_port;
    }

    // 4. Validate host is not empty
    if (host.empty()) {
        cerr << "❌ Host cannot be empty in URL: " << url << endl;
        return false;
    }

    cout << "🔗 Parsed URL - Scheme: " << (is_https ? "https" : "http") << ", Host: " << host
         << ", Port: " << port << ", Path: " << path << endl;
    return true;
}
