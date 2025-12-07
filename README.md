# cl-revenue-ops (experimental, in-progress)

A Revenue Operations Plugin for Core Lightning that provides intelligent fee management and profit-aware rebalancing.

## Overview

This plugin acts as a "Revenue Operations" layer that sits on top of the clboss automated manager. While clboss handles channel creation and node reliability, this plugin overrides clboss for fee setting and rebalancing decisions to maximize profitability based on economic principles rather than heuristics.

## Key Features

### Module 1: Flow Analysis & Sink/Source Detection
- Analyzes routing flow through each channel over a configurable time window
- Classifies channels as **SOURCE** (draining), **SINK** (filling), or **BALANCED**
- Uses bookkeeper plugin data when available, falls back to listforwards

### Module 2: Hill Climbing Fee Controller
- Implements a **Hill Climbing (Perturb & Observe)** algorithm for revenue-maximizing fee adjustment
- Uses **rate-based feedback** (revenue per hour) instead of absolute revenue for faster response
- Actively seeks the optimal fee point where `Revenue = Volume × Fee` is maximized
- Includes **wiggle dampening** to reduce step size on direction reversals
- **Volatility reset**: Detects large revenue shifts (>50%) and resets step size for aggressive re-exploration
- Applies profitability multipliers based on channel health
- Never drops below economic floor (based on channel costs)

### Module 3: EV-Based Rebalancing with Opportunity Cost
- Only executes rebalances with positive expected value
- **Weighted opportunity cost**: Accounts for lost revenue from draining source channel
- **Dynamic liquidity targeting**: Different targets based on flow state
  - SOURCE channels: Target 85% outbound (keep them full to earn)
  - BALANCED channels: Target 50% outbound (standard equilibrium)
  - SINK channels: Target 15% outbound (they fill themselves for free)
- **Persistent failure tracking**: Failure counts survive plugin restarts (prevents retry storms)
- **Last Hop Cost estimation**: Uses `listchannels` to get peer's actual fee policy toward us
- **Adaptive failure backoff**: Exponential cooldown for channels that keep failing
- **Global Capital Controls**: Prevents over-spending with two safety checks:
  - **Daily Budget**: Limits total rebalancing fees to a configurable amount per 24 hours
  - **Wallet Reserve**: Aborts rebalancing if total spendable funds (on-chain + channel local balance) fall below minimum threshold
- Sets strict budget caps to ensure profitability
- Supports both circular and sling rebalancer plugins

### Module 4: Channel Profitability Analyzer
- Tracks costs per channel (opening costs + rebalancing costs)
- Tracks revenue per channel (routing fees earned)
- Calculates **marginal ROI** to evaluate incremental investment value
- Classifies channels as **PROFITABLE**, **BREAK_EVEN**, **UNDERWATER**, or **ZOMBIE**
- Integrates with fee controller and rebalancer for smarter decisions

## Architecture

```
┌────────────────────────────────────────────────────────────────────────┐
│                        cl-revenue-ops Plugin                           │
├────────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  ┌─────────────────┐      ┌─────────────────┐                          │
│  │  Flow Analyzer  │      │  Profitability  │                          │
│  │  (Sink/Source)  │      │    Analyzer     │                          │
│  └────────┬────────┘      └────────┬────────┘                          │
│           │                        │                                   │
│           ▼                        │                                   │
│  ┌─────────────────┐               │                                   │
│  │    Database     │◀──────────────┘                                   │
│  │  (flow states,  │                                                   │
│  │   costs, fees)  │                                                   │
│  └────────┬────────┘                                                   │
│           │                                                            │
│     ┌─────┴─────┐                                                      │
│     ▼           ▼                                                      │
│  ┌──────────────────────┐      ┌──────────────────────┐                │
│  │   PID Fee Controller │      │    EV Rebalancer     │                │
│  │ (flow + profitability│      │ (flow + profitability│                │
│  │      multipliers)    │      │      checks)         │                │
│  └──────────┬───────────┘      └──────────┬───────────┘                │
│             │                             │                            │
│             ▼                             ▼                            │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                      Clboss Manager                             │   │
│  │                 (Manager-Override Pattern)                      │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│             │                             │                            │
└─────────────┼─────────────────────────────┼────────────────────────────┘
              ▼                             ▼
       ┌──────────────┐              ┌──────────────┐
       │   setchan    │              │   circular   │
       │   -nelfee    │              │    /sling    │
       └──────────────┘              └──────────────┘
              │
              ▼
       ┌──────────────┐
       │   clboss     │
       │   unmanage   │
       └──────────────┘
```

## Installation

### Prerequisites

1. Core Lightning node running
2. Python 3.8+
3. bookkeeper plugin enabled (recommended) or just listforwards
4. A rebalancer plugin installed (circular or sling)
5. clboss (optional, for full integration)

### Install Steps

```bash
# Clone or copy the plugin
cd ~/.lightning/plugins
git clone <repo-url> cl-revenue-ops
cd cl-revenue-ops

# Install dependencies
pip install -r requirements.txt

# Make the plugin executable
chmod +x cl-revenue-ops.py

# Start the plugin dynamically
lightning-cli plugin start ~/.lightning/plugins/cl-revenue-ops/cl-revenue-ops.py

# Or add to config for automatic loading
echo "plugin=~/.lightning/plugins/cl-revenue-ops/cl-revenue-ops.py" >> ~/.lightning/config
```

## Configuration Options

All options can be set via `lightning-cli` at startup or in your config file:

| Option | Default | Description |
|--------|---------|-------------|
| `revenue-ops-db-path` | `~/.lightning/revenue_ops.db` | SQLite database path |
| `revenue-ops-flow-interval` | `3600` | Flow analysis interval (seconds) |
| `revenue-ops-fee-interval` | `1800` | Fee adjustment interval (seconds) |
| `revenue-ops-rebalance-interval` | `900` | Rebalance check interval (seconds) |
| `revenue-ops-target-flow` | `100000` | Target daily flow per channel (sats) |
| `revenue-ops-min-fee-ppm` | `10` | Minimum fee floor (PPM) |
| `revenue-ops-max-fee-ppm` | `5000` | Maximum fee ceiling (PPM) |
| `revenue-ops-rebalance-min-profit` | `10` | Min profit to trigger rebalance (sats) |
| `revenue-ops-flow-window-days` | `7` | Days to analyze for flow |
| `revenue-ops-clboss-enabled` | `true` | Enable clboss integration |
| `revenue-ops-rebalancer` | `circular` | Rebalancer plugin (circular/sling) |
| `revenue-ops-daily-budget-sats` | `5000` | Max rebalancing fees per 24 hours (sats) |
| `revenue-ops-min-wallet-reserve` | `1000000` | Min total funds to keep in reserve (sats) |
| `revenue-ops-dry-run` | `false` | Log actions without executing |

*Note: The Hill Climbing fee controller manages its own internal state (step size, direction) automatically. PID parameters are kept for legacy compatibility but are not actively used.*

Example config:

```
# ~/.lightning/config
revenue-ops-target-flow=200000
revenue-ops-min-fee-ppm=50
revenue-ops-dry-run=true  # Test mode
```

## RPC Commands

### `revenue-status`
Get current plugin status and recent activity.

```bash
lightning-cli revenue-status
```

### `revenue-analyze [channel_id]`
Run flow analysis on demand.

```bash
# Analyze all channels
lightning-cli revenue-analyze

# Analyze specific channel
lightning-cli revenue-analyze 123x456x0
```

### `revenue-set-fee channel_id fee_ppm`
Manually set a channel fee (with clboss unmanage).

```bash
lightning-cli revenue-set-fee 123x456x0 500
```

### `revenue-rebalance from_channel to_channel amount_sats [max_fee_sats]`
Manually trigger a rebalance with profit constraints.

```bash
lightning-cli revenue-rebalance 123x456x0 789x012x1 500000
```

### `revenue-clboss-status`
Check clboss integration status.

```bash
lightning-cli revenue-clboss-status
```

### `revenue-profitability [channel_id]`
Get channel profitability analysis.

```bash
# Get summary of all channels grouped by profitability class
lightning-cli revenue-profitability

# Analyze specific channel
lightning-cli revenue-profitability 123x456x0
```

Returns:
- **profitable**: Positive ROI, earning more than costs
- **break_even**: ROI near zero
- **underwater**: Negative ROI, losing money
- **zombie**: No routing activity at all

### `revenue-remanage peer_id [tag]`
Re-enable clboss management for a peer.

```bash
lightning-cli revenue-remanage 03abc...def lnfee
```

## Manager-Override Pattern

This plugin uses a "Manager-Override" pattern to coexist with clboss:

1. **Detection**: Before changing any channel state, check if clboss is managing the peer
2. **Override**: Call `clboss-unmanage` for the specific tag (e.g., `lnfee`)
3. **Action**: Make our changes (set fee, trigger rebalance)
4. **Track**: Record what we've unmanaged for later reversion if needed

This allows:
- clboss to handle channel creation and peer selection (what it's good at)
- revenue-ops to handle fee optimization and profitable rebalancing (our specialty)

## How It Works

### Flow Analysis

Every hour (configurable), the plugin:

1. Queries bookkeeper or listforwards for the past 7 days
2. Calculates net flow: `FlowRatio = (SatsOut - SatsIn) / Capacity`
3. Classifies channels:
   - `FlowRatio > 0.5`: **SOURCE** (draining out) - These are money printers!
   - `FlowRatio < -0.5`: **SINK** (filling up) - These fill for free
   - Otherwise: **BALANCED**

### Hill Climbing Fee Control

Every 30 minutes (configurable), the plugin:

1. **Perturb**: Make a small fee change in a direction
2. **Observe**: Measure the resulting revenue rate (sats/hour) since last change
3. **Compare**: Is revenue rate better or worse than before?
4. **Decide**:
   - If Revenue Rate Increased: Keep going in the same direction
   - If Revenue Rate Decreased: Reverse direction (we went too far)
5. **Dampening**: On direction reversal, reduce step size (wiggle dampening)
6. Enforce floor (economic minimum) and ceiling constraints
7. Apply profitability multipliers based on channel health

### EV Rebalancing

Every 15 minutes (configurable), the plugin:

1. Identifies channels low on outbound liquidity
2. **CRITICAL: Checks flow state first**
   - If target is a **SINK**: SKIP (it fills itself for free!)
   - If target is a **SOURCE**: HIGH PRIORITY (keep it full!)
3. **Dynamic Liquidity Targeting**:
   - SOURCE channels: Target 85% outbound
   - BALANCED channels: Target 50% outbound
   - SINK channels: Target 15% outbound (only if critically low)
4. **Estimates inbound fee with Last Hop Cost**:
   - Queries `listchannels(source=peer_id)` to get peer's fee policy toward us
   - Adds network buffer for intermediate hops
5. **Calculates weighted opportunity cost**:
   - Source fee weighted by source channel's turnover rate
   - Spread = OutboundFee - InboundFee - WeightedOpportunityCost
6. Only executes if spread is positive and profit > minimum
7. **Adaptive failure backoff**: Exponential cooldown for failing channels
8. Calls circular via RPC with strict `maxppm` constraint

### Circular Integration (Strategist & Driver Pattern)

This plugin acts as the **Strategist** while circular is the **Driver**:

```python
# We calculate the EV constraint
max_ppm = int((max_fee_msat / amount_msat) * 1_000_000)

# We tell circular what to do via RPC
result = plugin.rpc.circular(
    outgoing_scid,    # Channel to drain
    incoming_scid,    # Channel to fill
    amount_msat,      # How much to move
    max_ppm,          # THE KEY CONSTRAINT - circular won't exceed this
    retry_count       # Number of retries
)
```

This separation of concerns means:
- **revenue-ops** handles the economics (when to rebalance, how much to pay)
- **circular** handles the mechanics (pathfinding, HTLC management)

### Anti-Thrashing Protection

After a successful rebalance, the plugin keeps the peer unmanaged from clboss's rebalancing logic. This prevents clboss from immediately "fixing" the channel balance and wasting the fees we just paid.

### Channel Profitability Analysis

The profitability analyzer tracks the economic performance of each channel:

**Cost Tracking:**
- Opening costs from **bookkeeper plugin** (`bkpr-listaccountevents` onchain_fee events)
- Channel open timestamps from bookkeeper `channel_open` events (or estimated from SCID block height)
- Rebalancing costs from bookkeeper invoice events with `fees_msat`
- Falls back to database cache, then config estimate if bookkeeper unavailable

**Revenue Tracking:**
- Routing fees earned from forwards through the channel

**Marginal ROI Analysis:**
- Calculates marginal ROI: return on the *last* unit of investment
- Helps decide whether additional investment (rebalancing) is worthwhile
- Channels with declining marginal ROI may not benefit from more capital

**Classification Logic:**
- **PROFITABLE**: ROI > 0% and net profit > 0
- **BREAK_EVEN**: ROI between -5% and 5%
- **UNDERWATER**: Negative ROI (costs exceed revenue)
- **ZOMBIE**: No routing activity detected

**Integration with Other Modules:**

1. **Fee Controller**: Applies profitability multipliers
   - Profitable channels: 1.0x (standard fees)
   - Break-even channels: 1.1x (slightly higher to improve margins)
   - Underwater channels: 1.2x (higher fees to recover costs)
   - Zombie channels: 1.15x (higher to make any activity worthwhile)

2. **Rebalancer**: Filters candidates by profitability
   - Skips ZOMBIE channels (no point investing in dead channels)
   - Skips deeply UNDERWATER channels (ROI < -50%)
   - Proceeds with caution on mildly underwater channels

## Monitoring

Check the database for historical data:

```bash
sqlite3 ~/.lightning/revenue_ops.db "SELECT * FROM fee_changes ORDER BY timestamp DESC LIMIT 10;"
sqlite3 ~/.lightning/revenue_ops.db "SELECT * FROM rebalance_history ORDER BY timestamp DESC LIMIT 10;"
```

## Troubleshooting

### Plugin won't start
- Check Python version: `python3 --version` (need 3.8+)
- Install dependencies: `pip install pyln-client`
- Check logs: `lightning-cli getlog debug | grep revenue`

### Fees not changing
- Ensure `dry-run` is `false`
- Check if clboss is reverting (enable `clboss-enabled`)
- Verify flow analysis has run: `lightning-cli revenue-status`

### Rebalances not happening
- Check if spread is positive: `lightning-cli revenue-status`
- Verify rebalancer plugin is installed
- Check minimum profit threshold

## License

MIT License

## Contributing

Contributions welcome! Please open issues or PRs on the repository.
