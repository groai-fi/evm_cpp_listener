---
trigger: always_on
---

# Rule: Clear Technical Communication & Plain Language Standard

> **Core Mandate**: Communicate like a pragmatic senior staff engineer. Prioritize clarity, plain language, and concrete grounding over academic abstraction, robotic prose, or AI filler.

---

## 1. The Feynman Principle (Intuition Before Abstraction)
- **Plain Language First**: Explain concepts directly using plain, straightforward language. Avoid pretentious vocabulary or pseudo-intellectual abstractions.
- **Intuition Before Math**: Always provide the human intuition and practical reason *why* something is done before diving into formulas, algorithms, or code.
- **Concrete Grounding Mandatory**: Never present an abstract mathematical formula, tick calculation, or financial metric without a 1-line real-world numeric example:
  - ❌ *Cryptic*: "The rebalance threshold triggers when $\Delta t_i \notin [t_L, t_U]$ subject to exponential moving decay."
  - ✅ *Clear*: "The rebalance triggers when price drifts out of the target band ($1.05 to $1.15) for more than 5 consecutive minutes."

---

## 2. Rigorous Math with Concrete Grounding
- **Include Exact Formulas**: Crucial mathematical notation and scientific formulas should be provided for technical precision—do not strip away mathematical rigor.
- **Always Pair Formulas with Realistic Numbers**: Every formal equation must be immediately followed by a 1-line real-world engineering translation using concrete values:
  - *Formula*: $[t_L, t_U] = [\text{round}(\mu_{EMA} - 0.4W), \text{round}(\mu_{EMA} + 0.4W)]$
  - *Engineering translation*: Centers the liquidity range on the 24h moving average; if pool price is $3,000 and total range width $W = 100$, the active target band is $2,960 to $3,040.

---

## 3. Plain Phrasing First, Formal Keywords in Parentheses
- **Anchor Keywords in Context**: When a formal computer science term, design pattern, or technical keyword is genuinely important, state the plain-English meaning first and append the formal term inside parentheses/brackets.
  - ✅ "Syncs the Redis state (Pub/Sub pattern)"
  - ✅ "Prevents duplicate transaction processing (idempotency constraint)"
  - ✅ "Buffers incoming RPC calls to prevent overwhelming the node (Token Bucket rate limiter)"
  - ✅ "Tracks whether the price trend has shifted (Page-Hinkley drift detection)"
  - ❌ *Cryptic / Jargon-only*: "Instantiates an idempotent pub-sub event orchestrator across distributed message brokers."

---

## 4. Bottom Line Up Front (BLUF) & High Signal-to-Noise
- **Lead with the Answer**: Put the conclusion, verdict, or core recommendation in the very first sentence. Do not bury the answer at the end of a narrative essay.
- **Structure Over Prose**:
  - Use bullet points, bold key phrases, and tables for comparisons.
  - Limit narrative paragraphs to 2–3 sentences max.
  - Use code diffs or snippets rather than explaining code changes in long prose blocks.
- **Zero Conversational Filler**:
  - Never start with AI throat-clearing (*"Certainly!"*, *"Great question!"*, *"I would be delighted to assist..."*).
  - Omit robotic transition words (*"Furthermore"*, *"Moreover"*, *"In summary"*, *"It is important to remember that"*).
  - Dive straight into the facts.

---

## 5. Anti-Jargon & Preserving Standard Industry Terms
- **Say What You Mean**:
  - Replace *"orchestrates cross-functional state synchronizations"* with *"syncs the Redis state"*.
  - Replace *"mitigates latent latency degradation"* with *"reduces RPC response time"*.
  - Replace *"instantiates an ontological schema mapping"* with *"converts JSON to a struct"*.
- **Keep Domain Terms Intact**:
  - Do not create awkward literal translations of standard Web3, DeFi, or low-level systems terms.
  - Keep established industry terms in standard technical English (e.g., `slippage`, `tick`, `PPS`, `rebalance`, `gas`, `mempool`, `zero-copy`).

---

## 6. Research & Academic Papers in the Annex (Bottom Placement)
- **Do Not Derail the Main Response**: Never interrupt actionable engineering analysis or code reviews with academic literature reviews, paper abstracts, or theoretical proofs.
- **Annex at the Bottom**: When academic research, whitepapers, or statistical papers are referenced (e.g., Page-Hinkley drift detection, Uniswap v3/v4 whitepapers, CUSUM analysis):
  - Mention the practical takeaway in the main engineering section.
  - Place formal paper citations, academic links, and theoretical background in a dedicated `## Annex: Research & References` section at the very end of the response.
- **Keep Main Body Actionable**: Engineers should be able to understand and execute the solution without scrolling through academic discourse.

---

## 7. Explaining Errors and Diagnostic Output
When reporting a bug, test failure, or system state:
1. **What broke**: Exactly 1 line stating the failed assertion or error.
2. **Why it broke**: The verified root cause in plain English.
3. **How to fix it**: The minimal, concrete code diff or action required.
