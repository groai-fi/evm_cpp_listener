---
trigger: always_on
---

# No Hardcoded Secrets and Dynamic .env Loading Standard

## 1. Zero-Tolerance Secret Policy
**You are strictly forbidden from adding hardcoded API keys, private keys, access tokens, client secrets, or RPC URLs containing secret tokens directly into any C++ source/header, TypeScript/JavaScript script, config, test, or code file.**

All sensitive credentials and network endpoints must be strictly configuration-driven and loaded dynamically from `.env*` files or process environment variables at runtime.

### Forbidden Anti-Patterns:

❌ **Hardcoded API keys, private keys, or secrets:**
```cpp
// C++ VIOLATION
const std::string BASESCAN_KEY = "YOUR_BASESCAN_KEY";
const std::string COINBASE_KEY = "YOUR_COINBASE_KEY";
const std::string COINBASE_SECRET = "YOUR_COINBASE_SECRET";
```
```typescript
// TypeScript VIOLATION
const BASESCAN_KEY = "YOUR_BASESCAN_KEY";
const COINBASE_KEY = "YOUR_COINBASE_KEY";
const COINBASE_SECRET = "YOUR_COINBASE_SECRET";
```

❌ **Secret tokens embedded in fallback URLs:**
```cpp
// C++ VIOLATION: Hardcoded token in fallback string leaks secrets
std::string rpc_url = env::get("BASE_RPC_URL", "https://base-mainnet.infura.io/v3/xxxxx");
```
```typescript
// TypeScript VIOLATION: Hardcoded token in fallback string leaks secrets
const RPC_URL = process.env.BASE_RPC_URL || "https://base-mainnet.infura.io/v3/xxxxx";
```

❌ **Committing real secrets into `.env.example`:**
```bash
# VIOLATION: Must use dummy placeholders in .env.example
BASE_RPC_URL=https://base-mainnet.g.alchemy.com/v2/real_secret_token
COINBASE_SECRET=real_secret_key_here
```

---

## 2. Dynamic `.env` Resolution Pattern
Standalone executables, diagnostics, event listeners, or test runners may be executed from various working directories (e.g. `build/`, `evm_listener/`, root).

Both C++ applications and TypeScript scripts must dynamically resolve `.env` files by walking up parent directories if environment variables are not already pre-set.

### C++ (C++17/C++20) Standard:
```cpp
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

namespace env {

inline void load_dotenv() {
    namespace fs = std::filesystem;
    std::error_code ec;
    fs::path current = fs::current_path(ec);
    if (ec) return;

    const std::vector<std::string> candidates = {".env", ".env.local.dev", ".env.local"};

    while (!current.empty() && current != current.root_path()) {
        for (const auto& candidate : candidates) {
            fs::path env_path = current / candidate;
            if (fs::exists(env_path, ec)) {
                std::ifstream file(env_path);
                std::string line;
                while (std::getline(file, line)) {
                    // Ignore empty lines and comments
                    if (line.empty() || line[0] == '#') continue;
                    auto delimiter_pos = line.find('=');
                    if (delimiter_pos != std::string::npos) {
                        std::string key = line.substr(0, delimiter_pos);
                        std::string value = line.substr(delimiter_pos + 1);
                        
                        // Trim whitespace and potential surrounding quotes
                        key.erase(0, key.find_first_not_of(" \t\r\n"));
                        key.erase(key.find_last_not_of(" \t\r\n") + 1);
                        value.erase(0, value.find_first_not_of(" \t\r\n\"'"));
                        value.erase(value.find_last_not_of(" \t\r\n\"'") + 1);

                        if (!key.empty() && !std::getenv(key.c_str())) {
#if defined(_WIN32)
                            _putenv_s(key.c_str(), value.c_str());
#else
                            setenv(key.c_str(), value.c_str(), 0); // 0 = do not overwrite existing
#endif
                        }
                    }
                }
            }
        }
        current = current.parent_path();
    }
}

inline std::string get(const std::string& key, const std::string& default_val = "") {
    const char* val = std::getenv(key.c_str());
    return val ? std::string(val) : default_val;
}

inline std::string require(const std::string& key) {
    const char* val = std::getenv(key.c_str());
    if (!val || std::string(val).empty()) {
        throw std::runtime_error("Missing required environment variable: " + key);
    }
    return std::string(val);
}

} // namespace env
```

### TypeScript (ESM / Node.js) Standard:
```typescript
import * as fs from "fs";
import * as path from "path";
import * as dotenv from "dotenv";

export function loadDotenv(): void {
  let currentDir = process.cwd();
  const root = path.parse(currentDir).root;
  const envCandidates = [".env", ".env.local.dev", ".env.local"];

  while (currentDir && currentDir !== root) {
    for (const file of envCandidates) {
      const envPath = path.join(currentDir, file);
      if (fs.existsSync(envPath)) {
        dotenv.config({ path: envPath });
      }
    }
    currentDir = path.dirname(currentDir);
  }
}

loadDotenv();

export function requireEnv(key: string): string {
  const val = process.env[key];
  if (!val) {
    throw new Error(`Missing required environment variable: ${key}`);
  }
  return val;
}
```

---

## 3. Standard Environment Variables & Safe Fallbacks

The project follows a standard multi-tier RPC naming convention per chain (`<CHAIN>_RPC_URL` -> `<CHAIN>_RPC_URL_ALCHEMY` -> `<CHAIN>_RPC_URL_ANKR`), plus explorer and exchange keys:

### Standard Environment Variable Names (from `evm_listener/.env`):

- **Ethereum Mainnet**:
  - `MAINNET_RPC_URL` (Infura / primary)
  - `MAINNET_RPC_URL_ALCHEMY`
  - `MAINNET_RPC_URL_ANKR`
  - `ETH_RPC_URL` (server-side PPS snapshots)
- **Arbitrum**:
  - `ARBITRUM_RPC_URL`
  - `ARBITRUM_RPC_URL_ALCHEMY`
  - `ARBITRUM_RPC_URL_ANKR`
- **Base**:
  - `BASE_RPC_URL`
  - `BASE_RPC_URL_ALCHEMY`
  - `BASE_RPC_URL_ANKR`
- **Optimism**:
  - `OPTIMISM_RPC_URL`
  - `OPTIMISM_RPC_URL_ALCHEMY`
  - `OPTIMISM_RPC_URL_ANKR`
- **Polygon**:
  - `POLYGON_RPC_URL`
  - `POLYGON_RPC_URL_ALCHEMY`
  - `POLYGON_RPC_URL_ANKR`
- **BNB Chain**:
  - `BNB_RPC_URL`
  - `BNB_RPC_URL_ALCHEMY`
  - `BNB_RPC_URL_ANKR`
- **Avalanche**:
  - `AVALANCHE_RPC_URL`
  - `AVALANCHE_RPC_URL_ALCHEMY`
  - `AVALANCHE_RPC_URL_ANKR`
- **Gnosis**:
  - `GNOSIS_RPC_URL`
  - `GNOSIS_RPC_URL_ALCHEMY`
  - `GNOSIS_RPC_URL_ANKR`
- **Scroll**:
  - `SCROLL_RPC_URL`
  - `SCROLL_RPC_URL_ALCHEMY`
  - `SCROLL_RPC_URL_ANKR`
- **Sepolia Testnet**:
  - `SEPOLIA_RPC_URL`
  - `SEPOLIA_RPC_URL_ALCHEMY`
  - `SEPOLIA_RPC_URL_ANKR`
- **Verification & Explorer API Keys**:
  - `BASESCAN_API_KEY`
  - `ETHERSCAN_API_KEY`
- **Exchange / API Credentials**:
  - `COINBASE_KEY`
  - `COINBASE_SECRET`

---

### Multi-Provider Cascade & Safe Public Fallbacks:

When configuring RPC endpoints, cascade across configured providers before falling back to public unauthenticated endpoints (or throwing if authenticated access is required):

**C++:**
```cpp
// Cascading lookup: Infura -> Alchemy -> Ankr -> Safe Public Unauthenticated Endpoint
std::string base_rpc = env::get("BASE_RPC_URL");
if (base_rpc.empty()) base_rpc = env::get("BASE_RPC_URL_ALCHEMY");
if (base_rpc.empty()) base_rpc = env::get("BASE_RPC_URL_ANKR");
if (base_rpc.empty()) base_rpc = "https://mainnet.base.org"; // Safe public endpoint
```

**TypeScript:**
```typescript
// Cascading lookup: Infura -> Alchemy -> Ankr -> Safe Public Unauthenticated Endpoint
const BASE_RPC_URL: string = process.env.BASE_RPC_URL 
  || process.env.BASE_RPC_URL_ALCHEMY 
  || process.env.BASE_RPC_URL_ANKR 
  || "https://mainnet.base.org";
```

---

### Fail Fast on Missing Credentials:
If a component strictly requires authenticated credentials (e.g., Coinbase trading API, explorer keys for verification), throw an explicit error rather than silently continuing with corrupt state or dummy keys:

**C++:**
```cpp
const std::string coinbase_key = env::require("COINBASE_KEY");
const std::string coinbase_secret = env::require("COINBASE_SECRET");

const char* basescan_key = std::getenv("BASESCAN_API_KEY");
if (!basescan_key || std::string(basescan_key).empty()) {
    std::cerr << "Warning: BASESCAN_API_KEY not set. Rate limits will apply.\n";
}
```

**TypeScript:**
```typescript
const COINBASE_KEY = requireEnv("COINBASE_KEY");
const COINBASE_SECRET = requireEnv("COINBASE_SECRET");

const BASESCAN_KEY = process.env.BASESCAN_API_KEY;
if (!BASESCAN_KEY) {
  console.warn("Warning: BASESCAN_API_KEY not set in .env. Rate limits will apply.");
}
```

---

## 4. Pre-Run & Pre-Commit Verification Checklist
Before compiling, running, creating, or editing any C++ or TypeScript code:
1. [ ] **No Raw Secrets:** No private keys, bearer tokens, API keys, or Coinbase secrets are written as string literals in C++ or TS/JS files.
2. [ ] **No Secret Fallbacks:** No `https://.../v3/<KEY>` or `https://.../v2/<KEY>` URLs exist as default fallback strings.
3. [ ] **Exact Variable Names:** All environment lookups match the exact canonical variable names from [`evm_listener/.env`](file:///Users/jonathan/MyCode/GroAI.fi/evm_cpp_listener/evm_listener/.env) (e.g. `MAINNET_RPC_URL_ALCHEMY`, `BASE_RPC_URL_ANKR`, `COINBASE_KEY`, `COINBASE_SECRET`).
4. [ ] **Dynamic `.env` Loading:** `env::load_dotenv()` (C++) or `loadDotenv()` (TypeScript) is initialized early in the program lifecycle / entry point to resolve `.env*` from the project tree.
5. [ ] **Explicit Errors:** Missing required credentials produce clean, informative configuration errors and fail fast.