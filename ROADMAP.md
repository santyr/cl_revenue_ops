# cl-revenue-ops Roadmap:

This document outlines the development path to move `cl-revenue-ops` from a "Power User" tool to an "Enterprise Grade" routing engine suitable for managing high-liquidity nodes.

## Phase 1: Capital Safety & Controls (In Progress)
*Objective: Prevent the algorithm from over-spending on fees or exhausting operating capital during high-volatility periods.*

- [x] **Global Daily Budgeting**: Implement a hard cap on total rebalancing fees paid per 24-hour rolling window.
- [x] **Wallet Reserve Protection**: Suspend all operations if on-chain or off-chain liquid funds drop below a safe reserve threshold.
- [ ] **Kelly Criterion Sizing**: (Optional) Dynamically scale rebalance budget based on the statistical certainty of the channel's revenue stream.

## Phase 2: Observability (High Priority)
*Objective: "You cannot manage what you cannot measure." Provide real-time visualization of algorithmic decisions.*

- [ ] **Prometheus Metrics Exporter**: Expose a local HTTP endpoint (or `.prom` file writer) to output time-series data:
    - Current Fee PPM per channel
    - Calculated Revenue Rate (sats/hr)
    - Marginal ROI
    - Rebalancing costs vs. Expected Profit
- [ ] **Decision Logging**: Create a structured event log (JSON lines) separate from the debug log for auditing algorithmic choices.

## Phase 3: Traffic Intelligence
*Objective: Optimize for quality liquidity and filter out noise/spam.*

- [ ] **HTLC Slot Awareness**: 
    - Monitor `htlc_max_concurrent` usage.
    - Mark channels with >80% slot usage as `CONGESTED`.
    - Prevent rebalancing into congested channels (liquidity cannot be used).
    - Apply fee premiums to congested channels.
- [ ] **Reputation-Weighted Analysis**:
    - Track HTLC failure rates per peer.
    - Discount volume from peers with high failure rates (spam/probing) in the Hill Climbing algorithm.
    - Optimize fees based on *settled* revenue potential, not just attempted volume.

## Phase 4: Stability & Scaling
*Objective: Reduce network noise and handle high throughput.*

- [ ] **Deadband Hysteresis**:
    - Detect "Market Calm" (low revenue variance).
    - Enter "Sleep Mode" for stable channels to reduce gossip noise (`channel_update` spam).
- [ ] **Async Job Queue**:
    - Refactor `rebalancer.py` to decouple decision-making from execution.
    - Allow concurrent rebalancing attempts (if supported by the underlying rebalancer plugin).

---
*Roadmap updated: December 07, 2025*