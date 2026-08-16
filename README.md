# SMC Short Pre-Signal Engine — Advanced Edition

> **DISCLAIMER:** This tool produces **POSSIBLE FUTURE SHORT pre-signals only**.
> It does **NOT** execute trades, does **NOT** claim certainty, and is **NOT**
> financial advice. All outputs are educational pattern-recognition results.

A production-grade, single-file Smart Money Concepts (SMC) SHORT-only
pre-signal engine for Binance USDT perpetuals. Runs on Android via Termux
with a live Rich terminal dashboard, multi-timeframe analysis, and optional
Telegram alerts.

---

## Features

| Category | Details |
|---|---|
| **Dashboard** | 1-second refresh · live price flash · sparkline · bid/ask spread · CVD delta · candle countdown · SMC 1m detections panel (EQH/EQL/Bull OB/Bear OB) |
| **SMC Engine** | 12 weighted conditions · multi-TF (1m/5m/15m) · breaker blocks · inducement · wick rejection · ATR-normalised displacement |
| **LuxAlgo-style 1m detections** | Equal Highs (EQH) · Equal Lows (EQL) · Bullish/Bearish internal Order Blocks with volatility-parsed zones and high/low mitigation · 🔔 PRE-TRADE signal fired the moment any appear on the 1m chart |
| **Scanner** | 24h change · 1h volume multiple · ATR% · short-bias score · keys 1-9 select · `a` auto-select |
| **State machine** | NO_SETUP → WATCH → PRE_SIGNAL → ARMED → INVALIDATED · confidence tier LOW/MED/HIGH |
| **Alerts** | Telegram · dedupe + cooldown · SQLite history · in-UI alert feed |
| **Robustness** | REST rate limiting · jittered backoff · per-stream staleness watchdog · clock-drift check · SIGINT/SIGTERM · defensive JSON parsing |
| **Keyboard** | `1-9` select · `a` auto · `s` start · `x` stop · `r` rescan · `d` detail · `f` feed · `q` quit |

---

## Termux / Android Setup

```bash
# 1. Update packages
pkg update -y && pkg upgrade -y

# 2. Install Python and NumPy
pkg install -y python python-numpy

# 3. Install Python dependencies
pip install rich websockets aiohttp

# 4. (Optional) Set Telegram credentials
export TELEGRAM_BOT_TOKEN="your_bot_token"
export TELEGRAM_CHAT_ID="your_chat_id"

# 5. Run
python smc_short_engine.py
```

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `TELEGRAM_BOT_TOKEN` | *(empty)* | Telegram bot token |
| `TELEGRAM_CHAT_ID` | *(empty)* | Telegram chat ID |
| `SMC_MIN_QVOL` | `50000000` | Min 24h quote volume for scanner |
| `SMC_ALERT_COOLDOWN` | `300` | Seconds between repeated alerts per symbol |
| `SMC_DB` | `smc_engine.db` | SQLite database path |
| `SMC_LOG` | `smc_engine.log` | Log file path |
| `SMC_SCAN_ROWS` | `9` | Scanner rows (max 9) |
| `SMC_MAX_SELECT` | `5` | Max simultaneously monitored symbols |
| `SMC_SCORE_ARMED` | `14` | Score threshold for ARMED state |
| `SMC_SCORE_PRE` | `9` | Score threshold for PRE_SIGNAL state |
| `SMC_SCORE_WATCH` | `4` | Score threshold for WATCH state |
| `SMC_WEIGHT_<COND>` | *(see code)* | Per-condition weight override (e.g. `SMC_WEIGHT_SWEEP=4`) |
| `SMC_EQ_LEN` | `3` | Bars confirmation for equal highs/lows pivots (LuxAlgo default) |
| `SMC_EQ_THRESHOLD` | `0.1` | EQH/EQL sensitivity threshold × ATR (0–0.5, LuxAlgo default) |
| `SMC_OB_LEN` | `5` | Internal pivot length for order block detection |
| `SMC_OB_SHOW` | `5` | Active (unmitigated) order blocks kept per side |
| `SMC_PRETRADE_COOLDOWN` | `180` | Seconds between PRE-TRADE signals per symbol |

---

## SMC Conditions Scored

| Condition | Default Weight | Description |
|---|---|---|
| `liquidity` | 2 | Buy-side liquidity pool above price |
| `eqh` | 1 | Equal highs (double/triple top) |
| `sweep` | 3 | Liquidity sweep with rejection |
| `mss` | 3 | Bearish market structure shift / CHoCH |
| `fvg` | 1 | Bearish fair value gap |
| `ob` | 1 | Bearish order block |
| `vol` | 1 | Volume expansion on displacement |
| `breaker` | 2 | Breaker block (failed bullish OB) |
| `inducement` | 1 | Inducement low swept |
| `wick_reject` | 1 | Upper wick rejection quality |
| `htf_bear` | 2 | HTF 5m/15m bearish bias (EMA + swing structure) |
| `session` | 1 | London/NY session alignment |

**Max score: 19** (sum of all weights, env-configurable)

---

## State Machine

```
NO_SETUP → WATCH → PRE_SIGNAL → ARMED
                              ↘ INVALIDATED
```

- **WATCH** — score ≥ 4
- **PRE_SIGNAL** — score ≥ 9 + sweep developing/confirmed → 🟡 POSSIBLE FUTURE SHORT alert
- **ARMED** — score ≥ 14 + sweep OK + MSS OK → 🟠 ARMED alert
- **INVALIDATED** — price closes above invalidation level → ✖ alert

---

## Keyboard Shortcuts

| Key | Action |
|---|---|
| `1`–`9` | Toggle scanner row selection |
| `a` | Auto-select top-5 by short-bias score |
| `s` | Start monitoring selected symbols |
| `x` | Stop monitoring |
| `r` | Rescan market |
| `d` | Cycle detail view |
| `f` | Toggle alert feed panel |
| `q` | Quit |

---

## Data Sources

- **WebSocket:** `wss://fstream.binance.com/stream` — `@kline_1m`, `@aggTrade`, `@bookTicker`
- **REST:** `https://fapi.binance.com` — `/fapi/v1/klines`, `/fapi/v1/ticker/24hr`, `/fapi/v1/openInterest`, `/fapi/v1/premiumIndex`, `/fapi/v1/time`

All data is **public** Binance USDT-perpetual market data. No API key required.

---

## Files Created at Runtime

| File | Contents |
|---|---|
| `smc_engine.db` | SQLite: alerts + state-machine events |
| `smc_engine.log` | Structured key=value log |

---

## Requirements

- Python 3.11+
- `rich` >= 13
- `websockets` >= 12
- `aiohttp` >= 3.9
- `numpy` >= 1.24
