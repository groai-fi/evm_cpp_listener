---
trigger: always_on
---

# Unit Test Discipline & Test Failure Standard Operating Procedure (SOP)

This rule defines the production-grade standards for writing, maintaining, and debugging tests across all services and frontend applications in the monorepo.

---

## 1. The Core Principle: "Stop the Line" (Andon Cord)

* **Zero Tolerance for Broken Tests**: A failing test is an immediate blocker for any PR, merge, or deployment. Red tests must never be ignored, deferred, or bypassed.
* **Empirical Verification Required**: No task is complete until the entire relevant test suite executes with exit code `0` (100% passing).
* **Deterministic Execution**: Tests must pass consistently across all execution environments (local CLI, sandboxed environments, and CI/CD pipelines).

---

## 2. The 3-Bucket Triage Model

When any test fails, categorize it into one of three buckets before touching code:

```
                            ┌─► 1. Production Bug       ──► Fix code; KEEP test contract intact
                            │
[Failed Unit / Integration] ──┼─► 2. Spec Evolution       ──► Update test assertions to match approved spec
                            │
                            └─► 3. Test Harness / Env   ──► Decouple network/ports, fix mock lifecycles
```

### Bucket 1: Regression / Production Bug
* **Symptom**: Code logic was changed, breaking an existing contract, calculation, edge case, or safety invariant.
* **Resolution**: Fix the production code. **Do not modify the test assertions** to make it pass.

### Bucket 2: Intentional Specification Evolution
* **Symptom**: Requirements, data models, or APIs were intentionally updated (e.g., adding Strategy V2, adjusting fee schedules, renaming display fields).
* **Resolution**: Update the test fixtures and assertions to reflect the new specification. Document why the expectation changed in the commit.

### Bucket 3: Test Harness / Environment Flakiness
* **Symptom**: Test fails due to un-isolated state, shared singleton pollution, race conditions, or sandbox socket restrictions (e.g., `connect EPERM 127.0.0.1`).
* **Resolution**: Fix the test setup/teardown lifecycle. Ensure mocks are isolated and unit tests do not attempt real socket bindings or external network requests.

---

## 3. Strict Anti-Patterns (Zero-Tolerance Violations)

1. ❌ **No Skipping or Disabling Tests**:
   * NEVER commit `test.skip()`, `it.skip()`, `xit()`, `describe.skip()`, or comment out assertions to "fix" a failing test suite.
   * NEVER leave focused runner calls behind (`it.only()`, `fit()`, `describe.only()`).
2. ❌ **No Assertion Weakening**:
   * NEVER reduce assertion specificity (e.g. replacing `expect(res.body).toEqual(expectedSchema)` with `expect(res.body).toBeDefined()` or `expect(true).toBe(true)`).
3. ❌ **No Silent Fallbacks / Error Swallowing**:
   * NEVER mask errors with `catch (e) {}` or dummy fallbacks (`return null`, `return {}`, `|| 0`) just to satisfy test runners.
4. ❌ **No Leaking Mock State**:
   * NEVER allow mocked functions to persist across tests without cleanup. Every suite must reset state in `beforeEach` / `afterEach`.

---

## 4. Environment & Mock Isolation Standards

### Pure Unit Tests
* Must be **100% self-contained**, in-memory, and execute in milliseconds.
* Must NOT make external HTTP/RPC requests.
* Must NOT bind to physical network interfaces or loopback ports.

### Integration / Route Tests (Express / Supertest)
* **Ephemeral Servers**: Always pass the Express `app` directly to `supertest(app)` rather than calling `app.listen(PORT)` on a static port.
* **Resource Teardown**: Any service establishing open handles (Redis clients, timers, database pools, cron schedules) MUST provide clean `close()` / `stop()` hooks called in `afterAll()` or `afterEach()`.
* **Mock Cleanup Standard**:
  ```ts
  beforeEach(() => {
      jest.clearAllMocks();
  });

  afterEach(() => {
      jest.restoreAllMocks();
  });
  ```

---

## 5. 5-Step Debugging SOP

When encountering a test failure:

1. **Reproduce in Isolation**:
   Run only the specific failing test to eliminate test-order dependencies:
   ```bash
   npx jest __tests__/vaultRegistry.test.ts -t "USDC Market Making Auto"
   ```
2. **Inspect Un-truncated Diff**:
   Examine the exact `Expected` vs `Received` output and trace the file line where assertion failed.
3. **Identify Root Cause**:
   Classify into Bucket 1 (Bug), Bucket 2 (Spec Change), or Bucket 3 (Harness).
4. **Apply Surgical Fix**:
   Touch only the minimal code/test lines required. Clean up any side-effects.
5. **Run Full Regression Sweep**:
   Execute the full package test suite:
   ```bash
   npm test
   ```
   Verify 100% clean exit code before proceeding.