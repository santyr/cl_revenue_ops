# cl-revenue-ops Roadmap:

This document outlines the development path to move `cl-revenue-ops` from a "Power User" tool to an "Enterprise Grade" routing engine suitable for managing high-liquidity nodes.

## Phase 1: Capital Safety & Controls (Completed)
*Objective: Prevent the algorithm from over-spending on fees or exhausting operating capital during high-volatility periods.*

- [x] **Global Daily Budgeting**: Implement a hard cap on total rebalancing fees paid per 24-hour rolling window.
- [x] **Wallet Reserve Protection**: Suspend all operations if on-chain or off-chain liquid funds drop below a safe reserve threshold.
- [x] **Kelly Criterion Sizing**: Dynamically scale rebalance budget based on the statistical certainty of the peer's reliability (Win Probability) and Profitability (Odds).

## Phase 2: Observability (In Progress)
*Objective: "You cannot manage what you cannot measure." Provide real-time visualization and auditing of algorithmic decisions.*

- [x] **Prometheus Metrics Exporter**: Expose a local HTTP endpoint (or `.prom` file writer) to output time-series data.
- [x] **Real-Time Metrics**: Updated event hooks to push metrics instantly, removing dashboard lag.
- [ ] **Decision Logging**: Create a structured event log (`decisions.jsonl`) separate from the debug log for auditing *why* specific fee/rebalance decisions were made.

## Phase 3: Traffic Intelligence (Completed)
*Objective: Optimize for quality liquidity and filter out noise/spam.*

- [x] **HTLC Slot Awareness**: Monitor usage and mark congested channels (>80% utilization).
- [x] **Reputation Tracking**: Track HTLC failure rates per peer in database.
- [x] **Reputation-Weighted Fees**: Discount volume from spammy peers in the Hill Climbing algorithm.
- [x] **Reputation Logic Refinements**: Implemented Laplace Smoothing and Time Decay.

## Phase 4: Stability & Scaling (In Progress)
*Objective: Reduce network noise and handle high throughput.*

- [x] **Deadband Hysteresis**:
    - Detect "Market Calm" (low revenue variance).
    - Enter "Sleep Mode" for stable channels to reduce gossip noise.
    - Wake up immediately on revenue spikes.
- [ ] **Async Job Queue**:
    - Refactor `rebalancer.py` to decouple decision-making from execution.
    - Allow concurrent rebalancing attempts (if supported by the underlying rebalancer plugin).

---
*Roadmap updated: December 08, 2025*