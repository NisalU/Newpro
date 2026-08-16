#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SMC SHORT PRE-SIGNAL ENGINE  -  Advanced Production Edition
Binance USDT-Perpetual  -  Multi-TF  -  SHORT-side analysis only
Termux/Android friendly  -  NO trading  -  NOT financial advice

DISCLAIMER: This tool produces POSSIBLE FUTURE SHORT pre-signals only.
It does NOT execute trades, does NOT claim certainty, and is NOT financial
advice. All signals are educational pattern-recognition outputs.

Environment variables (all optional):
  TELEGRAM_BOT_TOKEN   Telegram bot token for alert delivery
  TELEGRAM_CHAT_ID     Telegram chat ID for alert delivery
  SMC_MIN_QVOL         Min 24h quote volume filter (default 50_000_000)
  SMC_ALERT_COOLDOWN   Seconds between repeated alerts per symbol (default 300)
  SMC_DB               SQLite database path (default smc_engine.db)
  SMC_LOG              Log file path (default smc_engine.log)
  SMC_SCAN_ROWS        Number of scanner rows shown (default 9, max 9)
  SMC_MAX_SELECT       Max simultaneously monitored symbols (default 5)
  SMC_SCORE_ARMED      Score threshold for ARMED state (default 14)
  SMC_SCORE_PRE        Score threshold for PRE_SIGNAL state (default 9)
  SMC_SCORE_WATCH      Score threshold for WATCH state (default 4)
  SMC_WEIGHT_*         Per-condition weight overrides (see WEIGHTS dict)

Termux setup:
  pkg update -y && pkg upgrade -y
  pkg install -y python python-numpy
  pip install rich websockets aiohttp
  export TELEGRAM_BOT_TOKEN=xxx TELEGRAM_CHAT_ID=yyy
  python smc_short_engine.py

Keyboard shortcuts (cbreak mode, Termux-safe):
  1-9   Toggle scanner row selection
  a     Auto-select top-5 by short-bias score
  s     Start monitoring selected symbols
  x     Stop monitoring
  r     Rescan market
  d     Cycle detail view (per selected symbol)
  f     Toggle alert feed panel
  q     Quit
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# stdlib
# ---------------------------------------------------------------------------
import asyncio
import contextlib
import json
import logging
import os
import random
import signal
import socket
import sqlite3
import ssl
import sys
import termios
import time
import tty
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Deque, Dict, List, Optional, Set, Tuple

# ---------------------------------------------------------------------------
# third-party
# ---------------------------------------------------------------------------
import aiohttp
import numpy as np
import websockets
from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

# ===========================================================================
# CONFIGURATION
# ===========================================================================

REST_BASE: str = os.getenv("SMC_REST_BASE", "https://fapi.binance.com")
WS_BASE: str   = os.getenv("SMC_WS_BASE",   "wss://fstream.binance.com/stream")
FORCE_IPV4: bool = os.getenv("SMC_FORCE_IPV4", "1") == "1"   # IPv6 often hangs on Android/Termux


def _ssl_context() -> ssl.SSLContext:
    """Build an SSL context, preferring certifi CA bundle (Termux-safe)."""
    try:
        import certifi
        return ssl.create_default_context(cafile=certifi.where())
    except Exception:
        return ssl.create_default_context()

KLINE_LIMIT: int        = 300    # rolling closed-candle buffer (1m)
HTF_LIMIT: int          = 100    # HTF kline buffer (5m / 15m)
SCAN_ROWS: int          = min(9, int(os.getenv("SMC_SCAN_ROWS", "9")))
MAX_SELECT: int         = int(os.getenv("SMC_MAX_SELECT", "5"))
STALE_SEC: float        = 45.0   # ws silence -> reconnect
SCAN_INTERVAL: float    = 120.0  # auto-rescan period (s)
OI_POLL_INTERVAL: float = 30.0   # open-interest poll period (s)
FUNDING_POLL_INTERVAL: float = 60.0
CLOCK_DRIFT_WARN_MS: int = 2000
REST_RATE_LIMIT_DELAY: float = 0.12   # ~8 req/s conservative
ALERT_FEED_MAX: int     = 40     # alert feed ring buffer size
SPARKLINE_LEN: int      = 12     # price sparkline width
AGGR_DELTA_WINDOW: int  = 60     # seconds of aggTrade CVD window

MIN_QVOL: float       = float(os.getenv("SMC_MIN_QVOL", "50000000"))
ALERT_COOLDOWN: int   = int(os.getenv("SMC_ALERT_COOLDOWN", "300"))
TG_TOKEN: str         = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TG_CHAT: str          = os.getenv("TELEGRAM_CHAT_ID", "").strip()
DB_PATH: str          = os.getenv("SMC_DB", "smc_engine.db")
LOG_PATH: str         = os.getenv("SMC_LOG", "smc_engine.log")

SCORE_ARMED: int = int(os.getenv("SMC_SCORE_ARMED", "14"))
SCORE_PRE: int   = int(os.getenv("SMC_SCORE_PRE",   "9"))
SCORE_WATCH: int = int(os.getenv("SMC_SCORE_WATCH",  "4"))

# ---------------------------------------------------------------------------
# LuxAlgo-style SMC detection settings (1m timeframe)
# ---------------------------------------------------------------------------
EQ_CONFIRM_LEN: int     = int(os.getenv("SMC_EQ_LEN", "3"))          # bars confirmation
EQ_THRESHOLD: float     = float(os.getenv("SMC_EQ_THRESHOLD", "0.1"))  # x ATR sensitivity
OB_INTERNAL_LEN: int    = int(os.getenv("SMC_OB_LEN", "5"))          # internal pivot length
OB_SHOW_LAST: int       = int(os.getenv("SMC_OB_SHOW", "5"))         # active OBs kept per side
PRETRADE_COOLDOWN: int  = int(os.getenv("SMC_PRETRADE_COOLDOWN", "180"))  # s between pre-trade alerts


def _w(key: str, default: int) -> int:
    """Read per-condition weight from env (SMC_WEIGHT_<KEY>)."""
    return int(os.getenv(f"SMC_WEIGHT_{key.upper()}", str(default)))


WEIGHTS: Dict[str, int] = {
    "liquidity":   _w("liquidity",   2),
    "eqh":         _w("eqh",         1),
    "sweep":       _w("sweep",       3),
    "mss":         _w("mss",         3),
    "fvg":         _w("fvg",         1),
    "ob":          _w("ob",          1),
    "vol":         _w("vol",         1),
    "breaker":     _w("breaker",     2),
    "inducement":  _w("inducement",  1),
    "wick_reject": _w("wick_reject", 1),
    "htf_bear":    _w("htf_bear",    2),
    "session":     _w("session",     1),
}
MAX_SCORE: int = sum(WEIGHTS.values())

COND_LABELS: Dict[str, str] = {
    "liquidity":   "Buy-side Liquidity",
    "eqh":         "Equal Highs",
    "sweep":       "Liquidity Sweep",
    "mss":         "Bearish MSS/CHoCH",
    "fvg":         "Bearish FVG",
    "ob":          "Bearish Order Block",
    "vol":         "Volume Expansion",
    "breaker":     "Breaker Block",
    "inducement":  "Inducement",
    "wick_reject": "Wick Rejection",
    "htf_bear":    "HTF Bearish Bias",
    "session":     "Session Alignment",
}

# Trading sessions (UTC hours, inclusive start, exclusive end)
SESSIONS: Dict[str, Tuple[int, int]] = {
    "Asia":   (0,  8),
    "London": (7,  16),
    "NY":     (13, 22),
}

STATE_ORDER: List[str] = ["NO_SETUP", "WATCH", "PRE_SIGNAL", "ARMED", "INVALIDATED"]

# ===========================================================================
# LOGGING
# ===========================================================================

log: logging.Logger = logging.getLogger("smc")
log.setLevel(logging.INFO)
_fh = logging.FileHandler(LOG_PATH)
_fh.setFormatter(logging.Formatter(
    "ts=%(asctime)s level=%(levelname)s mod=%(name)s msg=%(message)s"
))
log.addHandler(_fh)

# ===========================================================================
# DATABASE / STORE
# ===========================================================================


class Store:
    """Thread-safe (single-thread asyncio) SQLite persistence layer."""

    def __init__(self, path: str) -> None:
        self.db: sqlite3.Connection = sqlite3.connect(path, check_same_thread=False)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS alerts(
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                ts           REAL    NOT NULL,
                symbol       TEXT    NOT NULL,
                state        TEXT    NOT NULL,
                score        INTEGER NOT NULL,
                max_score    INTEGER NOT NULL,
                zone_lo      REAL,
                zone_hi      REAL,
                invalidation REAL,
                confidence   TEXT,
                reason       TEXT
            )""")
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS events(
                id     INTEGER PRIMARY KEY AUTOINCREMENT,
                ts     REAL    NOT NULL,
                symbol TEXT    NOT NULL,
                kind   TEXT    NOT NULL,
                detail TEXT
            )""")
        self.db.commit()

    def save_alert(
        self,
        sym: str,
        state: str,
        score: int,
        zone: Tuple[float, float],
        inval: float,
        confidence: str,
        reason: str,
    ) -> None:
        """Persist an alert record."""
        self.db.execute(
            "INSERT INTO alerts(ts,symbol,state,score,max_score,zone_lo,zone_hi,"
            "invalidation,confidence,reason) VALUES(?,?,?,?,?,?,?,?,?,?)",
            (time.time(), sym, state, score, MAX_SCORE,
             zone[0], zone[1], inval, confidence, reason),
        )
        self.db.commit()

    def save_event(self, sym: str, kind: str, detail: str = "") -> None:
        """Persist a state-machine event."""
        self.db.execute(
            "INSERT INTO events(ts,symbol,kind,detail) VALUES(?,?,?,?)",
            (time.time(), sym, kind, detail),
        )
        self.db.commit()

    def recent_alerts(self, limit: int = ALERT_FEED_MAX) -> List[Dict[str, Any]]:
        """Return the most recent alerts ordered newest-first."""
        cur = self.db.execute(
            "SELECT ts,symbol,state,score,max_score,confidence FROM alerts "
            "ORDER BY ts DESC LIMIT ?",
            (limit,),
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    def close(self) -> None:
        with contextlib.suppress(Exception):
            self.db.close()


# ===========================================================================
# DATA MODELS
# ===========================================================================


@dataclass
class Candle:
    """OHLCV candle (closed only - no lookahead)."""

    t: int    # open time ms
    o: float
    h: float
    l: float
    c: float
    v: float

    @property
    def body(self) -> float:
        return abs(self.c - self.o)

    @property
    def upper_wick(self) -> float:
        return self.h - max(self.o, self.c)

    @property
    def lower_wick(self) -> float:
        return min(self.o, self.c) - self.l

    @property
    def is_bearish(self) -> bool:
        return self.c < self.o

    @property
    def candle_range(self) -> float:
        return self.h - self.l


class CondState(str, Enum):
    """Condition evaluation result."""
    OK   = "OK"
    DEV  = "DEV"
    WAIT = "WAIT"


class Confidence(str, Enum):
    """Setup confidence tier."""
    LOW  = "LOW"
    MED  = "MED"
    HIGH = "HIGH"


@dataclass
class Analysis:
    """Result of one SMC evaluation pass over closed candles."""

    conds:        Dict[str, CondState]
    score:        int
    max_score:    int
    zone:         Tuple[float, float]
    invalidation: float
    reasons:      List[str]
    close:        float
    atr:          float
    confidence:   Confidence
    htf_bias:     str          # "BEARISH" | "NEUTRAL" | "BULLISH"
    session:      str          # "Asia" | "London" | "NY" | "Off"
    premium_disc: str          # "PREMIUM" | "DISCOUNT" | "EQ"
    bar_count:    int          # bars since setup born


@dataclass
class TickData:
    """Live tick data updated from aggTrade / bookTicker streams."""

    price:      float = 0.0
    prev_price: float = 0.0
    bid:        float = 0.0
    ask:        float = 0.0
    spread:     float = 0.0
    cvd_delta:  float = 0.0    # cumulative volume delta (buy - sell) last window
    last_ts:    float = 0.0
    price_hist: Deque[float] = field(
        default_factory=lambda: deque(maxlen=SPARKLINE_LEN)
    )
    trade_hist: Deque[Tuple[float, float, float]] = field(
        default_factory=lambda: deque(maxlen=600)
    )

    def update_price(self, px: float) -> None:
        """Update live price and history."""
        self.prev_price = self.price
        self.price      = px
        self.price_hist.append(px)
        self.last_ts    = time.time()

    def update_book(self, bid: float, ask: float) -> None:
        """Update best bid/ask."""
        self.bid    = bid
        self.ask    = ask
        self.spread = ask - bid if ask > bid else 0.0

    def add_trade(self, ts: float, qty: float, is_buyer_maker: bool) -> None:
        """Record an aggTrade and recompute CVD."""
        buy_vol  = 0.0 if is_buyer_maker else qty
        sell_vol = qty if is_buyer_maker else 0.0
        self.trade_hist.append((ts, buy_vol, sell_vol))
        cutoff = time.time() - AGGR_DELTA_WINDOW
        self.cvd_delta = sum(
            b - s for t, b, s in self.trade_hist if t >= cutoff
        )

    @property
    def price_change_1s(self) -> float:
        return self.price - self.prev_price

    @property
    def spread_pct(self) -> float:
        mid = (self.bid + self.ask) / 2 if self.bid and self.ask else self.price
        return (self.spread / mid * 100) if mid else 0.0


@dataclass
class FundingOI:
    """Funding rate and open interest snapshot."""

    funding_rate: float = 0.0
    oi:           float = 0.0
    oi_prev:      float = 0.0
    last_funding: float = 0.0
    last_oi:      float = 0.0

    @property
    def oi_change_pct(self) -> float:
        if not self.oi_prev:
            return 0.0
        return (self.oi - self.oi_prev) / self.oi_prev * 100


@dataclass
class OBZone:
    """LuxAlgo-style order block zone (volatility-parsed high/low)."""

    top:      float
    bottom:   float
    bar_time: int
    bias:     int              # 1 = BULLISH, -1 = BEARISH
    mitigated: bool = False


@dataclass
class SMCDetections:
    """LuxAlgo-style 1m detections: EQH / EQL / order blocks."""

    eqh_level: Optional[float] = None
    eqh_time:  int             = 0
    eql_level: Optional[float] = None
    eql_time:  int             = 0
    bull_obs:  List[OBZone]    = field(default_factory=list)
    bear_obs:  List[OBZone]    = field(default_factory=list)

    @property
    def any_detected(self) -> bool:
        return (
            self.eqh_level is not None
            or self.eql_level is not None
            or bool(self.bull_obs)
            or bool(self.bear_obs)
        )


@dataclass
class Setup:
    """State machine for one symbol."""

    state:            str       = "NO_SETUP"
    born:             float     = 0.0
    bar_count:        int       = 0
    invalidation:     float     = 0.0
    zone:             Tuple[float, float] = (0.0, 0.0)
    inv_cool:         int       = 0
    last_alert_state: str       = ""
    last_alert_ts:    float     = 0.0
    last:             Optional[Analysis] = None
    confidence:       Confidence = Confidence.LOW
    # LuxAlgo-style 1m detections + pre-trade dedupe tracking
    detections:       Optional[SMCDetections] = None
    sig_eqh_time:     int       = 0
    sig_eql_time:     int       = 0
    sig_ob_times:     Set[int]  = field(default_factory=set)
    last_pretrade_ts: float     = 0.0


@dataclass
class ScanRow:
    """One row in the volatility scanner."""

    symbol:     str
    pct_24h:    float
    vol_ratio:  float
    atr_pct:    float
    score:      int
    short_bias: float   # 0-100


# ===========================================================================
# UTILITY HELPERS
# ===========================================================================


def _safe_float(val: Any, default: float = 0.0) -> float:
    """Defensive float conversion - never raises."""
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def _jitter(base: float, factor: float = 0.3) -> float:
    """Add random jitter to a backoff value to avoid thundering herd."""
    return base * (1 + random.uniform(-factor, factor))


def _classify_conn_error(exc: BaseException) -> str:
    """Translate a connection exception into an actionable message."""
    # HTTP status during WS handshake (websockets v11: InvalidStatusCode,
    # v12+: InvalidStatus with .response)
    status = getattr(exc, "status_code", None)
    if status is None:
        resp = getattr(exc, "response", None)
        status = getattr(resp, "status_code", None)
    if status in (451, 403):
        return (f"HTTP {status}: Binance blocks this region/IP. "
                "Try a VPN or set SMC_WS_BASE / SMC_REST_BASE")
    if status is not None:
        return f"handshake rejected (HTTP {status})"
    if isinstance(exc, ssl.SSLError):
        return ("SSL error: run 'pip install -U certifi' "
                "(Termux: pkg install ca-certificates)")
    if isinstance(exc, socket.gaierror) or "getaddrinfo" in str(exc):
        return "DNS lookup failed: check internet connection"
    if isinstance(exc, (asyncio.TimeoutError, TimeoutError)):
        return "connection timeout: network/firewall blocking wss:443"
    if isinstance(exc, (ConnectionRefusedError, ConnectionResetError, OSError)):
        return f"network error: {exc}"
    return str(exc) or exc.__class__.__name__


def _age_str(born: float) -> str:
    """Human-readable age string from epoch timestamp."""
    if not born:
        return "0s"
    d = int(time.time() - born)
    if d >= 3600:
        return f"{d // 3600}h{(d % 3600) // 60:02d}m"
    if d >= 60:
        return f"{d // 60}m{d % 60:02d}s"
    return f"{d}s"


def _sparkline(prices: Deque[float]) -> str:
    """Render a text sparkline from a price deque."""
    blocks = "\u2581\u2582\u2583\u2584\u2585\u2586\u2587\u2588"
    if len(prices) < 2:
        return "\u2500" * SPARKLINE_LEN
    mn, mx = min(prices), max(prices)
    rng = mx - mn or 1e-9
    return "".join(blocks[min(7, int((p - mn) / rng * 7))] for p in prices)


def _candle_close_countdown(interval_ms: int = 60_000) -> int:
    """Seconds until the next 1m candle closes."""
    now_ms  = int(time.time() * 1000)
    elapsed = now_ms % interval_ms
    return max(0, (interval_ms - elapsed) // 1000)


def _current_session() -> str:
    """Return the active trading session name by UTC hour."""
    h      = datetime.now(timezone.utc).hour
    active = [name for name, (start, end) in SESSIONS.items() if start <= h < end]
    return "/".join(active) if active else "Off"


def _atr(candles: List[Candle], period: int = 14) -> float:
    """Average True Range over last `period` candles."""
    if len(candles) < period + 1:
        return 1e-9
    trs = []
    for i in range(1, len(candles)):
        c_prev = candles[i - 1].c
        trs.append(max(
            candles[i].h - candles[i].l,
            abs(candles[i].h - c_prev),
            abs(candles[i].l - c_prev),
        ))
    return float(np.mean(trs[-period:])) or 1e-9


def _swing_highs(h: np.ndarray, lookback: int = 2) -> List[int]:
    """Return indices of fractal swing highs."""
    n = len(h)
    return [
        i for i in range(lookback, n - lookback)
        if all(h[i] >= h[i - j] for j in range(1, lookback + 1))
        and all(h[i] > h[i + j] for j in range(1, lookback + 1))
    ]


def _swing_lows(l: np.ndarray, lookback: int = 2) -> List[int]:
    """Return indices of fractal swing lows."""
    n = len(l)
    return [
        i for i in range(lookback, n - lookback)
        if all(l[i] <= l[i - j] for j in range(1, lookback + 1))
        and all(l[i] < l[i + j] for j in range(1, lookback + 1))
    ]


def _premium_discount(candles: List[Candle], window: int = 50) -> str:
    """Classify current price relative to recent range equilibrium."""
    if len(candles) < window:
        return "EQ"
    recent = candles[-window:]
    hi     = max(c.h for c in recent)
    lo     = min(c.l for c in recent)
    mid    = (hi + lo) / 2
    close  = candles[-1].c
    band   = (hi - lo) * 0.1
    if close > mid + band:
        return "PREMIUM"
    if close < mid - band:
        return "DISCOUNT"
    return "EQ"


# ===========================================================================
# LUXALGO-STYLE SMC DETECTION  (1m closed candles)
# ===========================================================================


def _pivot_points(arr: np.ndarray, length: int, is_high: bool) -> List[int]:
    """Confirmed pivot indices (ta.pivothigh/pivotlow semantics)."""
    n = len(arr)
    out: List[int] = []
    for i in range(length, n - length):
        left  = arr[i - length:i]
        right = arr[i + 1:i + length + 1]
        if is_high:
            if arr[i] > left.max() and arr[i] > right.max():
                out.append(i)
        else:
            if arr[i] < left.min() and arr[i] < right.min():
                out.append(i)
    return out


def _detect_equal_extremes(
    candles: List[Candle],
    atr_val: float,
) -> Tuple[Optional[Tuple[float, int]], Optional[Tuple[float, int]]]:
    """
    LuxAlgo-style Equal Highs / Equal Lows.

    Two consecutive confirmed pivots within EQ_THRESHOLD * ATR of each
    other form an EQH (liquidity above) or EQL (liquidity below).
    Returns ((level, bar_time) | None) for each side.
    """
    n = len(candles)
    if n < EQ_CONFIRM_LEN * 2 + 2:
        return None, None
    h = np.array([c.h for c in candles], dtype=float)
    l = np.array([c.l for c in candles], dtype=float)

    eqh: Optional[Tuple[float, int]] = None
    eql: Optional[Tuple[float, int]] = None

    ph = _pivot_points(h, EQ_CONFIRM_LEN, True)
    if len(ph) >= 2:
        a, b = ph[-2], ph[-1]
        if max(h[a], h[b]) - min(h[a], h[b]) < EQ_THRESHOLD * atr_val:
            eqh = (float(max(h[a], h[b])), candles[b].t)

    pl = _pivot_points(l, EQ_CONFIRM_LEN, False)
    if len(pl) >= 2:
        a, b = pl[-2], pl[-1]
        if max(l[a], l[b]) - min(l[a], l[b]) < EQ_THRESHOLD * atr_val:
            eql = (float(min(l[a], l[b])), candles[b].t)

    return eqh, eql


def _parsed_extremes(
    candles: List[Candle],
    atr_val: float,
) -> Tuple[np.ndarray, np.ndarray]:
    """
    LuxAlgo volatility filter: bars with range >= 2*ATR are 'volatile'
    and contribute their body instead of their wicks to OB zones.
    """
    p_hi = np.empty(len(candles), dtype=float)
    p_lo = np.empty(len(candles), dtype=float)
    for i, c in enumerate(candles):
        if c.candle_range >= 2.0 * atr_val:
            p_hi[i] = max(c.o, c.c)
            p_lo[i] = min(c.o, c.c)
        else:
            p_hi[i] = c.h
            p_lo[i] = c.l
    return p_hi, p_lo


def _detect_order_blocks(
    candles: List[Candle],
    atr_val: float,
) -> Tuple[List[OBZone], List[OBZone]]:
    """
    LuxAlgo-style internal order blocks on closed 1m candles.

    Bullish OB: on a structure break above a confirmed pivot high, the
    lowest parsed bar inside the leg becomes a demand zone.
    Bearish OB: on a structure break below a confirmed pivot low, the
    highest parsed bar inside the leg becomes a supply zone.

    Mitigation (HIGHLOW): bull OB removed once a later low trades below
    its bottom; bear OB removed once a later high trades above its top.
    Returns (bull_obs, bear_obs) - unmitigated, oldest first,
    capped at OB_SHOW_LAST per side.
    """
    n = len(candles)
    if n < OB_INTERNAL_LEN * 2 + 5:
        return [], []

    h  = np.array([c.h for c in candles], dtype=float)
    l  = np.array([c.l for c in candles], dtype=float)
    cl = np.array([c.c for c in candles], dtype=float)
    p_hi, p_lo = _parsed_extremes(candles, atr_val)

    bull: Dict[int, OBZone] = {}
    bear: Dict[int, OBZone] = {}

    for pi in _pivot_points(h, OB_INTERNAL_LEN, True):
        confirm = pi + OB_INTERNAL_LEN
        brk = next(
            (i for i in range(confirm, n) if cl[i] > h[pi]), -1
        )
        if brk <= pi + 1:
            continue
        j = min(range(pi, brk), key=lambda i: p_lo[i])
        # mitigation check: any later low below zone bottom
        if brk < n and float(l[brk:].min()) < p_lo[j]:
            continue
        bull[candles[j].t] = OBZone(
            top=float(p_hi[j]), bottom=float(p_lo[j]),
            bar_time=candles[j].t, bias=1,
        )

    for pi in _pivot_points(l, OB_INTERNAL_LEN, False):
        confirm = pi + OB_INTERNAL_LEN
        brk = next(
            (i for i in range(confirm, n) if cl[i] < l[pi]), -1
        )
        if brk <= pi + 1:
            continue
        j = max(range(pi, brk), key=lambda i: p_hi[i])
        if brk < n and float(h[brk:].max()) > p_hi[j]:
            continue
        bear[candles[j].t] = OBZone(
            top=float(p_hi[j]), bottom=float(p_lo[j]),
            bar_time=candles[j].t, bias=-1,
        )

    bull_list = sorted(bull.values(), key=lambda z: z.bar_time)[-OB_SHOW_LAST:]
    bear_list = sorted(bear.values(), key=lambda z: z.bar_time)[-OB_SHOW_LAST:]
    return bull_list, bear_list


def detect_smc_1m(candles: List[Candle]) -> SMCDetections:
    """Run all LuxAlgo-style 1m detections over closed candles."""
    det = SMCDetections()
    if len(candles) < 30:
        return det
    atr_val = _atr(candles, min(200, len(candles) - 1))
    eqh, eql = _detect_equal_extremes(candles, atr_val)
    if eqh:
        det.eqh_level, det.eqh_time = eqh
    if eql:
        det.eql_level, det.eql_time = eql
    det.bull_obs, det.bear_obs = _detect_order_blocks(candles, atr_val)
    return det


# ===========================================================================
# HTF CONFLUENCE ANALYSIS  (5m / 15m)
# ===========================================================================


def _htf_bias(candles_5m: List[Candle], candles_15m: List[Candle]) -> str:
    """
    Determine higher-timeframe directional bias.

    Returns 'BEARISH', 'NEUTRAL', or 'BULLISH'.
    Evaluates closed HTF candles only - no lookahead.
    """
    if len(candles_5m) < 20 or len(candles_15m) < 10:
        return "NEUTRAL"

    def _ema(arr: np.ndarray, period: int) -> np.ndarray:
        k   = 2 / (period + 1)
        out = np.empty_like(arr)
        out[0] = arr[0]
        for i in range(1, len(arr)):
            out[i] = arr[i] * k + out[i - 1] * (1 - k)
        return out

    c5  = np.array([c.c for c in candles_5m],  dtype=float)
    c15 = np.array([c.c for c in candles_15m], dtype=float)

    ema20_5  = _ema(c5,  20)[-1]
    ema50_5  = _ema(c5,  min(50, len(c5)))[-1]
    ema20_15 = _ema(c15, 20)[-1]

    bear_5  = c5[-1]  < ema20_5  and ema20_5  < ema50_5
    bear_15 = c15[-1] < ema20_15

    # HTF swing structure: lower highs on 15m
    h15    = np.array([c.h for c in candles_15m], dtype=float)
    sh15   = _swing_highs(h15, lookback=2)
    lower_highs = len(sh15) >= 2 and h15[sh15[-1]] < h15[sh15[-2]]

    bear_count = sum([bear_5, bear_15, lower_highs])
    if bear_count >= 2:
        return "BEARISH"
    if bear_count == 0:
        return "BULLISH"
    return "NEUTRAL"


# ===========================================================================
# CORE SMC ENGINE  (1m closed candles + HTF context)
# ===========================================================================


def analyze(
    candles: List[Candle],
    candles_5m: List[Candle],
    candles_15m: List[Candle],
    bar_count: int = 0,
) -> Optional[Analysis]:
    """
    Pure function over CLOSED candles only - zero lookahead bias.

    Evaluates 12 weighted SMC conditions and returns an Analysis.
    Returns None if insufficient data.

    Conditions scored:
      liquidity    - buy-side liquidity pool above price
      eqh          - equal highs (double/triple top liquidity)
      sweep        - liquidity sweep with rejection
      mss          - bearish market structure shift / CHoCH
      fvg          - bearish fair value gap
      ob           - bearish order block
      vol          - volume expansion on displacement
      breaker      - breaker block (failed OB)
      inducement   - inducement low swept
      wick_reject  - upper wick rejection quality
      htf_bear     - HTF 5m/15m bearish confluence
      session      - London/NY session alignment
    """
    n = len(candles)
    if n < 80:
        return None

    # numpy arrays
    o = np.array([c.o for c in candles], dtype=float)
    h = np.array([c.h for c in candles], dtype=float)
    l = np.array([c.l for c in candles], dtype=float)
    c = np.array([c.c for c in candles], dtype=float)
    v = np.array([c.v for c in candles], dtype=float)

    atr_val = _atr(candles, 14)
    close   = float(c[-1])

    # swing structure
    sh_idx = _swing_highs(h, lookback=2)
    sl_idx = _swing_lows(l,  lookback=2)

    recent_sh = [i for i in sh_idx if i >= n - 150]
    recent_sl = [i for i in sl_idx if i >= n - 150]

    conds: Dict[str, CondState] = {k: CondState.WAIT for k in WEIGHTS}
    reasons: List[str] = []

    # ------------------------------------------------------------------
    # 1. Buy-side liquidity
    # ------------------------------------------------------------------
    liq_level = 0.0
    if recent_sh:
        liq_level = float(max(h[i] for i in recent_sh))
        conds["liquidity"] = CondState.OK
        reasons.append(f"Buy-side liq @ {liq_level:.6g}")

    # ------------------------------------------------------------------
    # 2. Equal highs (liquidity pool)
    # ------------------------------------------------------------------
    if len(recent_sh) >= 2:
        highs = [h[i] for i in recent_sh[-6:]]
        found_eqh = False
        for ai in range(len(highs)):
            for bi in range(ai + 1, len(highs)):
                if abs(highs[ai] - highs[bi]) <= 0.18 * atr_val:
                    conds["eqh"] = CondState.OK
                    reasons.append("Equal highs (liquidity pool)")
                    found_eqh = True
                    break
            if found_eqh:
                break

    # ------------------------------------------------------------------
    # 3. Liquidity sweep
    # ------------------------------------------------------------------
    sweep_idx  = -1
    sweep_high = liq_level
    if liq_level > 0:
        for i in range(max(0, n - 40), n):
            if h[i] > liq_level + 0.015 * atr_val and c[i] < liq_level:
                sweep_idx  = i
                sweep_high = max(sweep_high, float(h[max(0, i - 3):n].max()))
        if sweep_idx >= 0:
            conds["sweep"] = CondState.OK
            reasons.append(f"Sweep + rejection @ {liq_level:.6g}")
        elif float(h[-15:].max()) >= liq_level - 0.3 * atr_val:
            conds["sweep"] = CondState.DEV
            reasons.append("Price tapping liquidity (sweep developing)")

    # ------------------------------------------------------------------
    # 4. Bearish displacement (feeds MSS / OB detection)
    # ------------------------------------------------------------------
    body_arr  = np.abs(c - o)
    mean_body = float(body_arr[-30:].mean()) or 1e-9
    start_idx = sweep_idx if sweep_idx >= 0 else n - 15
    disp_idx  = -1
    disp_strength = 0.0
    for i in range(max(1, start_idx), n):
        if c[i] < o[i] and body_arr[i] >= 1.25 * mean_body:
            strength = body_arr[i] / atr_val
            if strength > disp_strength:
                disp_idx      = i
                disp_strength = strength
    if disp_idx >= 0:
        reasons.append(f"Bearish displacement {disp_strength:.2f}xATR")

    # ------------------------------------------------------------------
    # 5. Bearish MSS / CHoCH
    # ------------------------------------------------------------------
    if sweep_idx >= 0:
        prior_lows = [i for i in sl_idx if i < sweep_idx]
        if prior_lows:
            key_low    = float(l[prior_lows[-1]])
            post_sweep = c[sweep_idx:]
            if len(post_sweep) and float(post_sweep.min()) < key_low:
                conds["mss"] = CondState.OK
                reasons.append(f"Bearish MSS below {key_low:.6g}")
    elif disp_idx >= 0 and recent_sl:
        key_low = float(l[recent_sl[-1]])
        if close < key_low:
            conds["mss"] = CondState.DEV
            reasons.append("CHoCH developing (no sweep yet)")

    # ------------------------------------------------------------------
    # 6. Bearish FVG  (high[i] < low[i-2])
    # ------------------------------------------------------------------
    fvg_zone: Optional[Tuple[float, float]] = None
    for i in range(max(2, start_idx), n):
        if h[i] < l[i - 2]:
            fvg_zone = (float(h[i]), float(l[i - 2]))
    if fvg_zone:
        conds["fvg"] = CondState.OK
        reasons.append(f"Bearish FVG {fvg_zone[0]:.6g}-{fvg_zone[1]:.6g}")

    # ------------------------------------------------------------------
    # 7. Bearish Order Block (last up-candle before displacement)
    # ------------------------------------------------------------------
    ob_zone: Optional[Tuple[float, float]] = None
    if disp_idx > 0:
        for i in range(disp_idx - 1, max(0, disp_idx - 12), -1):
            if c[i] > o[i]:
                ob_zone = (float(min(o[i], c[i])), float(h[i]))
                break
    if ob_zone:
        conds["ob"] = CondState.OK
        reasons.append(f"Bearish OB {ob_zone[0]:.6g}-{ob_zone[1]:.6g}")

    # ------------------------------------------------------------------
    # 8. Volume expansion
    # ------------------------------------------------------------------
    base_v       = float(v[-25:-3].mean()) or 1e-9
    recent_max_v = float(v[-5:].max())
    if recent_max_v >= 2.0 * base_v:
        conds["vol"] = CondState.OK
        reasons.append(f"Vol expansion {recent_max_v / base_v:.1f}x")
    elif recent_max_v >= 1.4 * base_v:
        conds["vol"] = CondState.DEV
        reasons.append(f"Vol building {recent_max_v / base_v:.1f}x")

    # ------------------------------------------------------------------
    # 9. Breaker block (prior bullish OB that price has broken below)
    # ------------------------------------------------------------------
    breaker_zone: Optional[Tuple[float, float]] = None
    if disp_idx > 0:
        for i in range(max(0, disp_idx - 30), disp_idx - 1):
            if c[i] > o[i]:
                ob_hi = float(h[i])
                ob_lo = float(min(o[i], c[i]))
                future_slice = h[i + 1:disp_idx + 1]
                if len(future_slice) and float(future_slice.max()) > ob_hi and close < ob_lo:
                    breaker_zone = (ob_lo, ob_hi)
                    break
    if breaker_zone:
        conds["breaker"] = CondState.OK
        reasons.append(f"Breaker block {breaker_zone[0]:.6g}-{breaker_zone[1]:.6g}")

    # ------------------------------------------------------------------
    # 10. Inducement (small swing low between two swing highs)
    # ------------------------------------------------------------------
    if len(recent_sh) >= 2 and recent_sl:
        sh_a, sh_b = recent_sh[-2], recent_sh[-1]
        ind_lows   = [i for i in recent_sl if sh_a < i < sh_b]
        if ind_lows:
            ind_low = float(l[ind_lows[-1]])
            if close < ind_low:
                conds["inducement"] = CondState.OK
                reasons.append(f"Inducement swept @ {ind_low:.6g}")
            else:
                conds["inducement"] = CondState.DEV
                reasons.append(f"Inducement target @ {ind_low:.6g}")

    # ------------------------------------------------------------------
    # 11. Wick rejection quality
    # ------------------------------------------------------------------
    wick_rejections = 0
    for i in range(max(0, n - 10), n):
        uw = candles[i].upper_wick
        bd = candles[i].body or 1e-9
        if uw >= 1.5 * bd and uw >= 0.3 * atr_val:
            wick_rejections += 1
    if wick_rejections >= 2:
        conds["wick_reject"] = CondState.OK
        reasons.append(f"Wick rejection x{wick_rejections}")
    elif wick_rejections == 1:
        conds["wick_reject"] = CondState.DEV
        reasons.append("Wick rejection x1")

    # ------------------------------------------------------------------
    # 12. HTF bearish bias (5m / 15m)
    # ------------------------------------------------------------------
    htf_bias_val = _htf_bias(candles_5m, candles_15m)
    if htf_bias_val == "BEARISH":
        conds["htf_bear"] = CondState.OK
        reasons.append("HTF 5m/15m bearish bias")
    elif htf_bias_val == "NEUTRAL":
        conds["htf_bear"] = CondState.DEV
        reasons.append("HTF neutral (mixed)")

    # ------------------------------------------------------------------
    # 13. Session alignment
    # ------------------------------------------------------------------
    session = _current_session()
    if session in ("London", "NY", "London/NY"):
        conds["session"] = CondState.OK
        reasons.append(f"Session: {session} (high liquidity)")
    elif session != "Off":
        conds["session"] = CondState.DEV
        reasons.append(f"Session: {session}")

    # ------------------------------------------------------------------
    # Score
    # ------------------------------------------------------------------
    score = 0
    for k, w in WEIGHTS.items():
        st = conds[k]
        if st == CondState.OK:
            score += w
        elif st == CondState.DEV:
            score += max(1, w // 2)

    # ------------------------------------------------------------------
    # Confidence tier
    # ------------------------------------------------------------------
    ok_count = sum(1 for s in conds.values() if s == CondState.OK)
    if score >= SCORE_ARMED and ok_count >= 7:
        confidence = Confidence.HIGH
    elif score >= SCORE_PRE and ok_count >= 4:
        confidence = Confidence.MED
    else:
        confidence = Confidence.LOW

    # ------------------------------------------------------------------
    # Zone and invalidation
    # ------------------------------------------------------------------
    zone: Tuple[float, float]
    if ob_zone:
        zone = ob_zone
    elif breaker_zone:
        zone = breaker_zone
    elif fvg_zone:
        zone = fvg_zone
    elif liq_level:
        zone = (liq_level, sweep_high if sweep_high > liq_level else liq_level + atr_val)
    else:
        zone = (0.0, 0.0)

    if zone[0] and zone[1]:
        zone = (min(zone[0], zone[1]), max(zone[0], zone[1]))

    if sweep_idx >= 0:
        inval = sweep_high + 0.12 * atr_val
    elif liq_level:
        inval = liq_level + 0.5 * atr_val
    else:
        inval = 0.0

    prem_disc = _premium_discount(candles, window=50)

    return Analysis(
        conds        = conds,
        score        = score,
        max_score    = MAX_SCORE,
        zone         = zone,
        invalidation = inval,
        reasons      = reasons,
        close        = close,
        atr          = atr_val,
        confidence   = confidence,
        htf_bias     = htf_bias_val,
        session      = session,
        premium_disc = prem_disc,
        bar_count    = bar_count,
    )


def next_state(a: Analysis) -> str:
    """Determine next state machine state from analysis result."""
    if (
        a.score >= SCORE_ARMED
        and a.conds["mss"]   == CondState.OK
        and a.conds["sweep"] == CondState.OK
    ):
        return "ARMED"
    if (
        a.score >= SCORE_PRE
        and a.conds["sweep"] in (CondState.OK, CondState.DEV)
    ):
        return "PRE_SIGNAL"
    if a.score >= SCORE_WATCH:
        return "WATCH"
    return "NO_SETUP"


# ===========================================================================
# TELEGRAM ALERTS
# ===========================================================================


def _build_tg_message(sym: str, state: str, a: Analysis, age: str) -> str:
    """Build a Telegram alert message string."""
    icons = {
        "PRE_SIGNAL":  "\U0001f7e1 POSSIBLE FUTURE SHORT",
        "ARMED":       "\U0001f7e0 ARMED - POSSIBLE FUTURE SHORT",
        "INVALIDATED": "\u2716 SETUP INVALIDATED",
    }
    conf_str = {
        Confidence.HIGH: "\U0001f534 HIGH",
        Confidence.MED:  "\U0001f7e1 MED",
        Confidence.LOW:  "\u26aa LOW",
    }
    header = icons.get(state, state)
    lines  = [
        header,
        f"{sym} - 1M - {a.session}",
        f"Confidence: {conf_str.get(a.confidence, a.confidence.value)}",
        f"HTF Bias: {a.htf_bias}",
        f"Zone: {a.premium_disc}",
        "",
    ]
    for k in WEIGHTS:
        st     = a.conds[k]
        mark   = "\u2713" if st == CondState.OK else ("\u26a0" if st == CondState.DEV else "\u25cb")
        suffix = " (developing)" if st == CondState.DEV else ""
        lines.append(f"{mark} {COND_LABELS[k]}{suffix}")
    lines += [
        "",
        f"Score: {a.score}/{a.max_score}",
    ]
    if a.zone[0]:
        lines.append(f"Potential Zone: {a.zone[0]:.6g} - {a.zone[1]:.6g}")
    if a.invalidation:
        lines.append(f"Invalidation: {a.invalidation:.6g}")
    lines += [
        f"Setup age: {age}",
        f"ATR: {a.atr:.6g}",
        "",
        "NOT A CONFIRMED TRADE SIGNAL.",
    ]
    return "\n".join(lines)


class Telegram:
    """Async Telegram message sender with queue and retry."""

    def __init__(self, session: aiohttp.ClientSession) -> None:
        self.session  = session
        self.q: asyncio.Queue[str] = asyncio.Queue(maxsize=100)
        self.enabled  = bool(TG_TOKEN and TG_CHAT)
        self.sent     = 0
        self.failed   = 0

    def push(self, text: str) -> None:
        """Enqueue a message for delivery (non-blocking)."""
        if not self.enabled:
            return
        with contextlib.suppress(asyncio.QueueFull):
            self.q.put_nowait(text)

    async def worker(self, stop: asyncio.Event) -> None:
        """Background coroutine that drains the message queue."""
        url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
        while not stop.is_set():
            try:
                text = await asyncio.wait_for(self.q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            for attempt in range(3):
                try:
                    async with self.session.post(
                        url,
                        json={"chat_id": TG_CHAT, "text": text},
                        timeout=aiohttp.ClientTimeout(total=12),
                    ) as resp:
                        if resp.status == 200:
                            self.sent += 1
                            break
                        log.warning("tg_status=%d attempt=%d", resp.status, attempt)
                except Exception as exc:
                    log.warning("tg_err=%s attempt=%d", exc, attempt)
                await asyncio.sleep(_jitter(2 ** attempt))
            else:
                self.failed += 1


# ===========================================================================
# REST RATE-LIMITED CLIENT
# ===========================================================================


class RateLimitedClient:
    """Thin wrapper around aiohttp.ClientSession with per-request delay."""

    def __init__(self, session: aiohttp.ClientSession) -> None:
        self._session  = session
        self._last_req = 0.0
        self._lock     = asyncio.Lock()

    async def get_json(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        timeout: float = 15.0,
    ) -> Any:
        """GET JSON with rate limiting."""
        async with self._lock:
            elapsed = time.time() - self._last_req
            if elapsed < REST_RATE_LIMIT_DELAY:
                await asyncio.sleep(REST_RATE_LIMIT_DELAY - elapsed)
            self._last_req = time.time()

        async with self._session.get(
            url,
            params=params,
            timeout=aiohttp.ClientTimeout(total=timeout),
        ) as resp:
            resp.raise_for_status()
            return await resp.json()


# ===========================================================================
# RICH UI HELPERS
# ===========================================================================

_STATE_STYLE: Dict[str, str] = {
    "NO_SETUP":    "dim",
    "WATCH":       "cyan",
    "PRE_SIGNAL":  "bold yellow",
    "ARMED":       "bold red",
    "INVALIDATED": "red",
    "PRE_TRADE":   "bold magenta",
}

_STATE_ICON: Dict[str, str] = {
    "NO_SETUP":    "\u2014 NO SETUP",
    "WATCH":       "\U0001f440 WATCHING",
    "PRE_SIGNAL":  "\U0001f7e1 POSSIBLE FUTURE SHORT",
    "ARMED":       "\U0001f7e0 ARMED - POSSIBLE FUTURE SHORT",
    "INVALIDATED": "\u2716 INVALIDATED",
}

_CONF_STYLE: Dict[Confidence, str] = {
    Confidence.HIGH: "bold red",
    Confidence.MED:  "bold yellow",
    Confidence.LOW:  "dim white",
}

_COND_MARK: Dict[CondState, str] = {
    CondState.OK:   "[green]\u2713[/]",
    CondState.DEV:  "[yellow]\u26a0[/]",
    CondState.WAIT: "[dim]\u25cb[/]",
}


_SPINNER_FRAMES = "\u280b\u2819\u2839\u2838\u283c\u2834\u2826\u2827\u2807\u280f"


def _spinner() -> str:
    """Animated braille spinner frame based on wall clock."""
    return _SPINNER_FRAMES[int(time.time() * 8) % len(_SPINNER_FRAMES)]


def _score_bar(score: int, max_s: int, width: int = 12) -> str:
    """Render a simple text progress bar for the score."""
    filled = int(score / max_s * width) if max_s else 0
    bar    = "\u2588" * filled + "\u2591" * (width - filled)
    pct    = score / max_s * 100 if max_s else 0
    color  = "red" if pct >= 70 else ("yellow" if pct >= 40 else "dim")
    return f"[{color}]{bar}[/] {score}/{max_s}"


def _score_gauge(score: int, width: int = 20) -> str:
    """Score gauge with WATCH / PRE_SIGNAL / ARMED threshold ticks."""
    if MAX_SCORE <= 0:
        return "-"
    filled = int(score / MAX_SCORE * width)
    chars: List[str] = []
    marks = {
        int(SCORE_WATCH / MAX_SCORE * width): "cyan",
        int(SCORE_PRE   / MAX_SCORE * width): "yellow",
        int(SCORE_ARMED / MAX_SCORE * width): "red",
    }
    fill_color = (
        "red" if score >= SCORE_ARMED
        else "yellow" if score >= SCORE_PRE
        else "cyan" if score >= SCORE_WATCH
        else "dim"
    )
    for i in range(width):
        if i in marks and i >= filled:
            chars.append(f"[{marks[i]}]\u2506[/]")
        elif i < filled:
            chars.append(f"[{fill_color}]\u2588[/]")
        else:
            chars.append("[dim]\u2591[/]")
    pct = score / MAX_SCORE * 100
    return "".join(chars) + f" [bold {fill_color}]{score}[/]/{MAX_SCORE} [dim]({pct:.0f}%)[/]"


def _next_state_info(score: int) -> str:
    """Describe distance to the next state threshold."""
    if score >= SCORE_ARMED:
        return "[bold red]MAX TIER - ARMED[/]"
    if score >= SCORE_PRE:
        return f"[red]+{SCORE_ARMED - score} pts to ARMED[/]"
    if score >= SCORE_WATCH:
        return f"[yellow]+{SCORE_PRE - score} pts to PRE_SIGNAL[/]"
    return f"[cyan]+{SCORE_WATCH - score} pts to WATCH[/]"


def _tick_age_str(tick: TickData) -> str:
    """Human age of last tick, colored by freshness."""
    if not tick.last_ts:
        return "[dim]no data[/]"
    age = time.time() - tick.last_ts
    if age < 3:
        return f"[green]\u25cf {age:.0f}s[/]"
    if age < 15:
        return f"[yellow]\u25cf {age:.0f}s[/]"
    return f"[red]\u25cf stale {age:.0f}s[/]"


def _px_color(tick: TickData) -> str:
    """Color string for live price based on 1s change."""
    if tick.price_change_1s > 0:
        return "green"
    if tick.price_change_1s < 0:
        return "red"
    return "white"


def _px_arrow(tick: TickData) -> str:
    """Direction arrow for last price move."""
    if tick.price_change_1s > 0:
        return "\u25b2"
    if tick.price_change_1s < 0:
        return "\u25bc"
    return "\u2500"


# ===========================================================================
# MAIN APPLICATION
# ===========================================================================


class App:
    """
    Main application orchestrator.

    Manages WebSocket streams, REST polling, SMC evaluation,
    Rich UI rendering, keyboard input, and Telegram alerts.
    """

    def __init__(self) -> None:
        self.console   = Console()
        self.stop      = asyncio.Event()
        self.session: Optional[aiohttp.ClientSession] = None
        self.client:  Optional[RateLimitedClient]     = None
        self.store     = Store(DB_PATH)
        self.tg:      Optional[Telegram]              = None

        # Scanner state
        self.scan:      List[ScanRow] = []
        self.scan_ts:   float         = 0.0
        self.scan_busy: bool          = False

        # Selection / monitoring
        self.selected:   Set[str]              = set()
        self.monitoring: bool                  = False
        self.conn_ok:    bool                  = False
        self.ws_task:    Optional[asyncio.Task] = None

        # Per-symbol data
        self.buffers_1m:  Dict[str, Deque[Candle]] = {}
        self.buffers_5m:  Dict[str, Deque[Candle]] = {}
        self.buffers_15m: Dict[str, Deque[Candle]] = {}
        self.ticks:       Dict[str, TickData]       = {}
        self.funding_oi:  Dict[str, FundingOI]      = {}
        self.setups:      Dict[str, Setup]           = {}
        self.ws_last_msg: Dict[str, float]           = {}

        # UI state
        self.detail:     Optional[str] = None
        self.show_feed:  bool          = False
        self.status_msg: str           = "press r to scan - 1-9 select - s start"
        self.alert_feed: Deque[str]    = deque(maxlen=ALERT_FEED_MAX)

        # Stats
        self.start_ts:       float = time.time()
        self.msg_count:      int   = 0
        self.reconnects:     int   = 0
        self.ws_latency_ms:  float = 0.0
        self.clock_drift_ms: float = 0.0
        self.ws_error:       str   = ""
        self.msg_times:      Deque[float] = deque(maxlen=400)

        self._kb_restore = lambda: None

    # -----------------------------------------------------------------------
    # Lifecycle
    # -----------------------------------------------------------------------

    async def run(self) -> None:
        """Main entry point - sets up everything and runs the event loop."""
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            with contextlib.suppress(NotImplementedError):
                loop.add_signal_handler(sig, self.stop.set)

        self.session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=20, ttl_dns_cache=300)
        )
        self.client = RateLimitedClient(self.session)
        self.tg     = Telegram(self.session)

        tg_task    = asyncio.create_task(self.tg.worker(self.stop))
        scan_task  = asyncio.create_task(self._scan_loop())
        clock_task = asyncio.create_task(self._clock_sync_loop())
        oi_task    = asyncio.create_task(self._oi_funding_loop())
        stale_task = asyncio.create_task(self._stale_watchdog())

        self._kb_restore = self._keyboard_on(loop)

        try:
            with Live(
                self._render(),
                console=self.console,
                refresh_per_second=4,
                screen=True,
                transient=False,
            ) as live:
                while not self.stop.is_set():
                    live.update(self._render())
                    await asyncio.sleep(0.25)
        finally:
            self.monitoring = False
            for t in (self.ws_task, scan_task, tg_task, clock_task,
                      oi_task, stale_task):
                if t:
                    t.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await t
            self._kb_restore()
            if self.session:
                await self.session.close()
            self.store.close()
            log.info("shutdown clean=1")

    # -----------------------------------------------------------------------
    # Keyboard
    # -----------------------------------------------------------------------

    def _keyboard_on(self, loop: asyncio.AbstractEventLoop):
        """Enable cbreak keyboard input (Termux-safe). Returns restore fn."""
        if not sys.stdin.isatty():
            return lambda: None
        try:
            fd  = sys.stdin.fileno()
            old = termios.tcgetattr(fd)
            tty.setcbreak(fd)
            loop.add_reader(fd, self._on_key, fd)

            def restore() -> None:
                with contextlib.suppress(Exception):
                    loop.remove_reader(fd)
                    termios.tcsetattr(fd, termios.TCSADRAIN, old)

            return restore
        except Exception as exc:
            log.warning("keyboard_setup_err=%s", exc)
            return lambda: None

    def _on_key(self, fd: int) -> None:
        """Handle a single keypress."""
        try:
            ch = os.read(fd, 4).decode(errors="ignore").strip()
        except OSError:
            return
        if not ch:
            return
        ch   = ch[0]
        loop = asyncio.get_running_loop()
        if ch == "q":
            self.stop.set()
        elif ch == "r":
            loop.create_task(self._refresh_scanner())
        elif ch == "s":
            self._start_monitoring()
        elif ch == "x":
            self._stop_monitoring()
        elif ch == "d":
            self._cycle_detail()
        elif ch == "f":
            self.show_feed = not self.show_feed
        elif ch == "a":
            self._auto_select()
        elif ch.isdigit() and ch != "0":
            self._toggle_selection(int(ch) - 1)

    def _toggle_selection(self, idx: int) -> None:
        """Toggle selection of scanner row by index."""
        if idx >= len(self.scan):
            return
        sym = self.scan[idx].symbol
        if sym in self.selected:
            self.selected.discard(sym)
            self.status_msg = f"deselected {sym}"
        elif len(self.selected) >= MAX_SELECT:
            self.status_msg = f"max {MAX_SELECT} coins selected"
            return
        else:
            self.selected.add(sym)
            self.status_msg = f"selected {sym}"
        self.store.save_event(sym, "selection", str(sym in self.selected))

    def _auto_select(self) -> None:
        """Auto-select top-N by short-bias score."""
        top = sorted(self.scan, key=lambda r: r.short_bias, reverse=True)[:MAX_SELECT]
        self.selected   = {r.symbol for r in top}
        self.status_msg = f"auto-selected {len(self.selected)} coins"

    def _cycle_detail(self) -> None:
        """Cycle the detail view through selected symbols."""
        syms = sorted(self.selected)
        if not syms:
            self.detail = None
            return
        opts: List[Optional[str]] = [None] + syms  # type: ignore[list-item]
        cur         = self.detail if self.detail in opts else None
        self.detail = opts[(opts.index(cur) + 1) % len(opts)]

    def _start_monitoring(self) -> None:
        """Start WebSocket monitoring for selected symbols."""
        if self.monitoring:
            return
        if not self.selected:
            self.status_msg = "select coins first (1-9 or a)"
            return
        self.monitoring = True
        self.status_msg = "monitoring started"
        self.ws_task    = asyncio.get_running_loop().create_task(self._ws_loop())
        log.info("monitor_start symbols=%s", ",".join(sorted(self.selected)))

    def _stop_monitoring(self) -> None:
        """Stop WebSocket monitoring."""
        if not self.monitoring:
            return
        self.monitoring = False
        self.status_msg = "monitoring stopped"
        if self.ws_task:
            self.ws_task.cancel()
        log.info("monitor_stop")

    # -----------------------------------------------------------------------
    # Scanner
    # -----------------------------------------------------------------------

    async def _scan_loop(self) -> None:
        """Periodic scanner refresh loop."""
        await self._refresh_scanner()
        while not self.stop.is_set():
            await asyncio.sleep(SCAN_INTERVAL)
            if not self.monitoring:
                await self._refresh_scanner()

    async def _refresh_scanner(self) -> None:
        """Fetch 24h ticker data and build the scanner list."""
        if self.scan_busy:
            return
        self.scan_busy  = True
        self.status_msg = "scanning market..."
        try:
            data = await self.client.get_json(f"{REST_BASE}/fapi/v1/ticker/24hr")
            rows = [
                d for d in data
                if isinstance(d, dict)
                and d.get("symbol", "").endswith("USDT")
                and "_" not in d.get("symbol", "")
                and _safe_float(d.get("quoteVolume")) >= MIN_QVOL
            ]
            rows.sort(
                key=lambda d: abs(_safe_float(d.get("priceChangePercent"))),
                reverse=True,
            )
            out: List[ScanRow] = []
            for d in rows[:20]:
                sym     = d["symbol"]
                pct     = _safe_float(d.get("priceChangePercent"))
                volx    = await self._vol_ratio(sym)
                atr_pct = await self._atr_pct(sym)
                # Short-bias score: negative 24h change, high volume, high ATR
                short_bias = (
                    max(0.0, -pct) * 5.0
                    + min(volx, 8.0) * 6.0
                    + min(atr_pct, 5.0) * 4.0
                )
                score = int(min(100, short_bias))
                out.append(ScanRow(sym, pct, volx, atr_pct, score, short_bias))
                if len(out) >= SCAN_ROWS:
                    break
            out.sort(key=lambda x: x.short_bias, reverse=True)
            self.scan    = out
            self.scan_ts = time.time()
            self.status_msg = f"scanner updated ({len(out)} coins)"
            log.info("scan_ok rows=%d", len(out))
        except Exception as exc:
            self.status_msg = f"scan failed: {exc}"
            log.warning("scan_err err=%s", exc)
        finally:
            self.scan_busy = False

    async def _vol_ratio(self, sym: str) -> float:
        """1h volume vs 24h average ratio."""
        try:
            k = await self.client.get_json(
                f"{REST_BASE}/fapi/v1/klines",
                params={"symbol": sym, "interval": "1h", "limit": 25},
            )
            vols = np.array([_safe_float(x[5]) for x in k])
            base = float(vols[:-1].mean()) or 1e-9
            return float(vols[-1] / base)
        except Exception:
            return 1.0

    async def _atr_pct(self, sym: str) -> float:
        """ATR as % of price (1h candles, 14-period)."""
        try:
            k = await self.client.get_json(
                f"{REST_BASE}/fapi/v1/klines",
                params={"symbol": sym, "interval": "1h", "limit": 20},
            )
            candles = [
                Candle(
                    int(x[0]),
                    _safe_float(x[1]), _safe_float(x[2]),
                    _safe_float(x[3]), _safe_float(x[4]),
                    _safe_float(x[5]),
                )
                for x in k
            ]
            a  = _atr(candles, 14)
            px = candles[-1].c or 1e-9
            return a / px * 100
        except Exception:
            return 0.0

    # -----------------------------------------------------------------------
    # REST pollers
    # -----------------------------------------------------------------------

    async def _oi_funding_loop(self) -> None:
        """Poll open interest and funding rate for selected symbols."""
        while not self.stop.is_set():
            for sym in list(self.selected):
                await self._fetch_oi(sym)
                await self._fetch_funding(sym)
            await asyncio.sleep(OI_POLL_INTERVAL)

    async def _fetch_oi(self, sym: str) -> None:
        """Fetch open interest for one symbol."""
        try:
            data   = await self.client.get_json(
                f"{REST_BASE}/fapi/v1/openInterest",
                params={"symbol": sym},
            )
            oi_new = _safe_float(data.get("openInterest"))
            fi     = self.funding_oi.setdefault(sym, FundingOI())
            fi.oi_prev = fi.oi
            fi.oi      = oi_new
            fi.last_oi = time.time()
        except Exception as exc:
            log.debug("oi_err sym=%s err=%s", sym, exc)

    async def _fetch_funding(self, sym: str) -> None:
        """Fetch funding rate for one symbol."""
        try:
            data = await self.client.get_json(
                f"{REST_BASE}/fapi/v1/premiumIndex",
                params={"symbol": sym},
            )
            fi = self.funding_oi.setdefault(sym, FundingOI())
            fi.funding_rate = _safe_float(data.get("lastFundingRate")) * 100
            fi.last_funding = time.time()
        except Exception as exc:
            log.debug("funding_err sym=%s err=%s", sym, exc)

    async def _clock_sync_loop(self) -> None:
        """Periodically check clock drift against Binance server time."""
        while not self.stop.is_set():
            try:
                t0   = time.time()
                data = await self.client.get_json(f"{REST_BASE}/fapi/v1/time")
                rtt  = time.time() - t0
                srv  = _safe_float(data.get("serverTime")) / 1000
                local = t0 + rtt / 2
                self.clock_drift_ms = (local - srv) * 1000
                if abs(self.clock_drift_ms) > CLOCK_DRIFT_WARN_MS:
                    log.warning("clock_drift_ms=%.0f", self.clock_drift_ms)
            except Exception as exc:
                log.debug("clock_sync_err=%s", exc)
            await asyncio.sleep(300)

    async def _stale_watchdog(self) -> None:
        """Detect per-stream staleness and trigger reconnect if needed."""
        while not self.stop.is_set():
            await asyncio.sleep(10)
            if not self.monitoring:
                continue
            now = time.time()
            for sym in list(self.selected):
                last = self.ws_last_msg.get(sym, now)
                if now - last > STALE_SEC:
                    log.warning("stale_stream sym=%s age=%.0fs", sym, now - last)
                    self.conn_ok = False
                    if self.ws_task and not self.ws_task.done():
                        self.ws_task.cancel()
                    self.ws_task = asyncio.get_running_loop().create_task(
                        self._ws_loop()
                    )
                    break

    # -----------------------------------------------------------------------
    # WebSocket
    # -----------------------------------------------------------------------

    async def _bootstrap_symbol(self, sym: str) -> None:
        """Fetch historical klines for a symbol (1m, 5m, 15m)."""
        for interval, buf_attr, limit in (
            ("1m",  "buffers_1m",  KLINE_LIMIT + 1),
            ("5m",  "buffers_5m",  HTF_LIMIT   + 1),
            ("15m", "buffers_15m", HTF_LIMIT   + 1),
        ):
            buf_dict: Dict[str, Deque[Candle]] = getattr(self, buf_attr)
            if sym in buf_dict and len(buf_dict[sym]) >= 60:
                continue
            try:
                k = await self.client.get_json(
                    f"{REST_BASE}/fapi/v1/klines",
                    params={"symbol": sym, "interval": interval, "limit": limit},
                )
                maxlen = KLINE_LIMIT if interval == "1m" else HTF_LIMIT
                buf: Deque[Candle] = deque(maxlen=maxlen)
                for row in k[:-1]:   # drop open (live) candle - no lookahead
                    buf.append(Candle(
                        int(row[0]),
                        _safe_float(row[1]), _safe_float(row[2]),
                        _safe_float(row[3]), _safe_float(row[4]),
                        _safe_float(row[5]),
                    ))
                buf_dict[sym] = buf
                log.info("bootstrap sym=%s tf=%s candles=%d", sym, interval, len(buf))
            except Exception as exc:
                log.warning("bootstrap_err sym=%s tf=%s err=%s", sym, interval, exc)

        self.setups.setdefault(sym, Setup())
        self.ticks.setdefault(sym, TickData())
        self.ws_last_msg[sym] = time.time()

    async def _preflight(self) -> bool:
        """REST connectivity check with actionable diagnosis before WS connect."""
        try:
            async with self.session.get(
                f"{REST_BASE}/fapi/v1/ping",
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status in (451, 403):
                    self.ws_error = (
                        f"HTTP {resp.status}: Binance blocks this region/IP. "
                        "Try a VPN or set SMC_WS_BASE / SMC_REST_BASE"
                    )
                    log.warning("preflight_geoblock status=%d", resp.status)
                    return False
                resp.raise_for_status()
            return True
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.ws_error = _classify_conn_error(exc)
            log.warning("preflight_err=%s", self.ws_error)
            return False

    async def _ws_loop(self) -> None:
        """Main WebSocket loop with exponential backoff and auto-reconnect."""
        backoff = 1.0
        while self.monitoring and not self.stop.is_set():
            symbols = sorted(self.selected)
            if not symbols:
                await asyncio.sleep(1)
                continue
            try:
                # Preflight REST check surfaces geo-block / DNS / SSL causes
                # instead of an eternal silent "RECONNECTING".
                if not await self._preflight():
                    self.conn_ok = False
                    await asyncio.sleep(_jitter(backoff))
                    backoff = min(backoff * 2, 60.0)
                    continue

                for sym in symbols:
                    await self._bootstrap_symbol(sym)

                # Build combined stream URL
                streams: List[str] = []
                for sym in symbols:
                    sl = sym.lower()
                    streams += [
                        f"{sl}@kline_1m",
                        f"{sl}@aggTrade",
                        f"{sl}@bookTicker",
                    ]
                url = f"{WS_BASE}?streams={'/'.join(streams)}"

                connect_kwargs: Dict[str, Any] = dict(
                    ping_interval=20,
                    ping_timeout=20,
                    max_queue=512,
                    open_timeout=15,
                    close_timeout=5,
                    ssl=_ssl_context(),
                )
                if FORCE_IPV4:
                    connect_kwargs["family"] = socket.AF_INET

                async with websockets.connect(url, **connect_kwargs) as ws:
                    self.conn_ok  = True
                    self.ws_error = ""
                    backoff       = 1.0
                    log.info("ws_connected symbols=%d streams=%d",
                             len(symbols), len(streams))

                    while self.monitoring and not self.stop.is_set():
                        if set(symbols) != self.selected:
                            log.info("ws_resubscribe selection_changed=1")
                            break
                        t0  = time.time()
                        raw = await asyncio.wait_for(ws.recv(), timeout=STALE_SEC)
                        self.ws_latency_ms = (time.time() - t0) * 1000
                        self.msg_count    += 1
                        self.msg_times.append(time.time())
                        try:
                            self._dispatch(json.loads(raw))
                        except Exception as exc:
                            log.debug("dispatch_err=%s", exc)

            except asyncio.CancelledError:
                break
            except asyncio.TimeoutError:
                self.conn_ok    = False
                self.reconnects += 1
                self.ws_error   = "stream silent/timeout - reconnecting"
                log.warning("ws_timeout reconnecting=1 backoff=%.1f", backoff)
                await asyncio.sleep(_jitter(backoff))
                backoff = min(backoff * 2, 30.0)
            except Exception as exc:
                self.conn_ok    = False
                self.reconnects += 1
                self.ws_error   = _classify_conn_error(exc)
                log.warning("ws_err=%s backoff=%.1f", self.ws_error, backoff)
                await asyncio.sleep(_jitter(backoff))
                backoff = min(backoff * 2, 60.0)

        self.conn_ok = False

    def _dispatch(self, msg: dict) -> None:
        """Route an incoming WebSocket message to the correct handler."""
        data   = msg.get("data", {})
        stream = msg.get("stream", "")
        if not isinstance(data, dict):
            return
        if "@kline_1m" in stream:
            self._on_kline(data)
        elif "@aggTrade" in stream:
            self._on_agg_trade(data)
        elif "@bookTicker" in stream:
            self._on_book_ticker(data)

    def _on_kline(self, data: dict) -> None:
        """Handle a kline WebSocket message."""
        k = data.get("k")
        if not isinstance(k, dict):
            return
        sym = k.get("s", "")
        if not sym:
            return
        self.ws_last_msg[sym] = time.time()
        tick = self.ticks.setdefault(sym, TickData())
        tick.update_price(_safe_float(k.get("c")))

        if not k.get("x"):          # candle not yet closed - tick only
            return

        buf = self.buffers_1m.get(sym)
        if buf is None:
            return
        candle = Candle(
            int(k.get("t", 0)),
            _safe_float(k.get("o")), _safe_float(k.get("h")),
            _safe_float(k.get("l")), _safe_float(k.get("c")),
            _safe_float(k.get("v")),
        )
        if buf and buf[-1].t == candle.t:
            return                  # duplicate guard
        buf.append(candle)
        self._evaluate(sym)

    def _on_agg_trade(self, data: dict) -> None:
        """Handle an aggTrade WebSocket message."""
        sym = data.get("s", "")
        if not sym:
            return
        self.ws_last_msg[sym] = time.time()
        tick = self.ticks.setdefault(sym, TickData())
        qty  = _safe_float(data.get("q"))
        is_buyer_maker = bool(data.get("m", False))
        ts   = _safe_float(data.get("T", 0)) / 1000
        tick.add_trade(ts, qty, is_buyer_maker)
        tick.update_price(_safe_float(data.get("p")))

    def _on_book_ticker(self, data: dict) -> None:
        """Handle a bookTicker WebSocket message."""
        sym = data.get("s", "")
        if not sym:
            return
        self.ws_last_msg[sym] = time.time()
        tick = self.ticks.setdefault(sym, TickData())
        tick.update_book(
            _safe_float(data.get("b")),
            _safe_float(data.get("a")),
        )

    # -----------------------------------------------------------------------
    # SMC evaluation
    # -----------------------------------------------------------------------

    def _evaluate(self, sym: str) -> None:
        """Run SMC analysis on closed candles and update state machine."""
        buf_1m  = list(self.buffers_1m.get(sym,  []))
        buf_5m  = list(self.buffers_5m.get(sym,  []))
        buf_15m = list(self.buffers_15m.get(sym, []))

        s = self.setups.setdefault(sym, Setup())
        s.bar_count += 1

        a = analyze(buf_1m, buf_5m, buf_15m, s.bar_count)
        if a is None:
            return

        s.last       = a
        s.confidence = a.confidence
        prev         = s.state

        # ------------------------------------------------------------------
        # LuxAlgo-style 1m detections -> PRE-TRADE signal
        # EQH / EQL / Bullish OB / Bearish OB on the 1m timeframe
        # ------------------------------------------------------------------
        det = detect_smc_1m(buf_1m)
        s.detections = det
        events: List[str] = []
        if det.eqh_level is not None and det.eqh_time != s.sig_eqh_time:
            s.sig_eqh_time = det.eqh_time
            events.append(f"EQH @ {det.eqh_level:.6g}")
        if det.eql_level is not None and det.eql_time != s.sig_eql_time:
            s.sig_eql_time = det.eql_time
            events.append(f"EQL @ {det.eql_level:.6g}")
        for z in det.bear_obs:
            if z.bar_time not in s.sig_ob_times:
                s.sig_ob_times.add(z.bar_time)
                events.append(f"Bearish OB {z.bottom:.6g}-{z.top:.6g}")
        for z in det.bull_obs:
            if z.bar_time not in s.sig_ob_times:
                s.sig_ob_times.add(z.bar_time)
                events.append(f"Bullish OB {z.bottom:.6g}-{z.top:.6g}")
        if len(s.sig_ob_times) > 400:            # prune dedupe memory
            s.sig_ob_times = set(sorted(s.sig_ob_times)[-100:])
        if events and (time.time() - s.last_pretrade_ts) > PRETRADE_COOLDOWN:
            s.last_pretrade_ts = time.time()
            self._fire_pretrade(sym, events, a)

        # Invalidation cooldown
        if s.inv_cool > 0:
            s.inv_cool -= 1
            if s.inv_cool == 0:
                s.state = "NO_SETUP"
                s.born  = 0.0
            return

        # Invalidation check for active setups
        if (
            prev in ("PRE_SIGNAL", "ARMED")
            and s.invalidation
            and a.close > s.invalidation
        ):
            s.state    = "INVALIDATED"
            s.inv_cool = 6
            self.store.save_event(sym, "invalidated", f"close={a.close:.6g}")
            log.info("invalidated sym=%s close=%.6g inval=%.6g",
                     sym, a.close, s.invalidation)
            if s.last_alert_state in ("PRE_SIGNAL", "ARMED"):
                self._fire_alert(sym, "INVALIDATED", a, s)
            return

        # State transition
        new = next_state(a)
        if new != "NO_SETUP" and s.born == 0.0:
            s.born = time.time()
        if new == "NO_SETUP":
            s.born      = 0.0
            s.bar_count = 0
        if new in ("PRE_SIGNAL", "ARMED"):
            s.invalidation = a.invalidation
            s.zone         = a.zone

        if new != prev:
            self.store.save_event(sym, "state", f"{prev}->{new} score={a.score}")
            log.info("state sym=%s prev=%s new=%s score=%d conf=%s",
                     sym, prev, new, a.score, a.confidence.value)
        s.state = new

        # Alert logic: only meaningful upgrades, deduped + cooldown
        upgraded = (
            prev in STATE_ORDER
            and new in STATE_ORDER
            and STATE_ORDER.index(new) > STATE_ORDER.index(prev)
        )
        if new in ("PRE_SIGNAL", "ARMED") and upgraded:
            fresh = (
                new != s.last_alert_state
                or (time.time() - s.last_alert_ts) > ALERT_COOLDOWN
            )
            if fresh:
                self._fire_alert(sym, new, a, s)

    def _fire_pretrade(self, sym: str, events: List[str], a: Analysis) -> None:
        """Fire a PRE-TRADE signal for fresh 1m EQH/EQL/OB detections."""
        reason = "; ".join(events)
        self.store.save_alert(
            sym, "PRE_TRADE", a.score, a.zone, a.invalidation,
            a.confidence.value, reason,
        )
        lines = [
            "\U0001f514 PRE-TRADE SIGNAL (1m SMC)",
            f"{sym} - 1M - {a.session}",
            "",
            "Detected:",
        ]
        lines += [f"  \u2022 {e}" for e in events]
        lines += [
            "",
            f"Price: {a.close:.6g}",
            f"HTF Bias: {a.htf_bias}",
            f"Zone: {a.premium_disc}",
            f"Score: {a.score}/{a.max_score}",
            "",
            "NOT A CONFIRMED TRADE SIGNAL.",
        ]
        if self.tg:
            self.tg.push("\n".join(lines))
        ts_str = datetime.now(timezone.utc).strftime("%H:%M:%S")
        self.alert_feed.appendleft(
            f"{ts_str} \U0001f514 {sym} PRE_TRADE {reason[:44]}"
        )
        self.status_msg = f"PRE-TRADE {sym}: {reason[:48]}"
        log.info("pretrade sym=%s events=%s", sym, reason)

    def _fire_alert(self, sym: str, state: str, a: Analysis, s: Setup) -> None:
        """Persist, queue Telegram, and update UI feed for an alert."""
        age    = _age_str(s.born)
        reason = "; ".join(a.reasons) or "-"
        self.store.save_alert(
            sym, state, a.score, a.zone, a.invalidation,
            a.confidence.value, reason,
        )
        msg = _build_tg_message(sym, state, a, age)
        if self.tg:
            self.tg.push(msg)
        s.last_alert_state = state
        s.last_alert_ts    = time.time()
        self.status_msg    = f"ALERT {sym} {state} {a.score}/{MAX_SCORE}"

        # Feed entry
        ts_str = datetime.now(timezone.utc).strftime("%H:%M:%S")
        icon   = {"PRE_SIGNAL": "\U0001f7e1", "ARMED": "\U0001f7e0",
                  "INVALIDATED": "\u2716"}.get(state, "-")
        self.alert_feed.appendleft(
            f"{ts_str} {icon} {sym} {state} {a.score}/{MAX_SCORE} "
            f"[{a.confidence.value}]"
        )
        log.info("alert sym=%s state=%s score=%d conf=%s zone=%s inval=%.6g",
                 sym, state, a.score, a.confidence.value, a.zone, a.invalidation)

    # -----------------------------------------------------------------------
    # Rendering
    # -----------------------------------------------------------------------

    def _render(self) -> Group:
        """Build the full Rich renderable for the current frame."""
        width  = self.console.width
        narrow = width < 60

        parts = [
            self._render_header(narrow),
            self._render_scanner(narrow),
            self._render_selected_bar(),
        ]

        if self.show_feed:
            parts.append(self._render_alert_feed())
        elif self.detail and self.detail in self.setups:
            parts.append(self._render_detail(self.detail))
        else:
            for sym in sorted(self.selected):
                parts.append(self._render_coin_row(sym, narrow))

        parts.append(self._render_footer(narrow))
        return Group(*parts)

    def _render_header(self, narrow: bool) -> Panel:
        """Render the top header panel."""
        uptime = int(time.time() - self.start_ts)
        h_u, m_u, s_u = uptime // 3600, (uptime % 3600) // 60, uptime % 60
        uptime_str = f"{h_u:02d}:{m_u:02d}:{s_u:02d}"

        # Rolling msg/s over the last 5 seconds -> feels live
        now      = time.time()
        recent   = sum(1 for t_ in self.msg_times if now - t_ <= 5.0)
        msg_rate = recent / 5.0

        if self.monitoring and self.conn_ok:
            status = Text(f"{_spinner()} LIVE", style="bold green")
        elif self.monitoring:
            status = Text(f"{_spinner()} RECONNECTING", style="bold yellow")
        else:
            status = Text("\u25cb IDLE", style="dim")

        drift_warn = ""
        if abs(self.clock_drift_ms) > CLOCK_DRIFT_WARN_MS:
            drift_warn = f"  \u26a0 clock drift {self.clock_drift_ms:+.0f}ms"

        if narrow:
            t = Text("SMC SHORT ENGINE\n", style="bold white", justify="center")
            t.append_text(status)
            if self.monitoring and self.conn_ok:
                t.append(f"  {msg_rate:.1f} msg/s", style="dim")
        else:
            t = Text(
                "SMC SHORT PRE-SIGNAL ENGINE  -  Binance USDT-Perp  -  SHORT only\n",
                style="bold white",
                justify="center",
            )
            t.append_text(status)
            t.append(
                f"  uptime {uptime_str}"
                f"  ws {self.ws_latency_ms:.0f}ms"
                f"  {msg_rate:.1f} msg/s"
                f"  reconnects {self.reconnects}"
                f"  session {_current_session()}"
                f"{drift_warn}",
                style="dim",
            )
        if self.monitoring and not self.conn_ok and self.ws_error:
            t.append(f"\n\u26a0 {self.ws_error}", style="bold red")
        panel_style = (
            "green" if self.monitoring and self.conn_ok
            else "yellow" if self.monitoring
            else "cyan"
        )
        return Panel(t, style=panel_style, padding=(0, 1))

    def _render_scanner(self, narrow: bool) -> Panel:
        """Render the volatility scanner panel."""
        tb = Table(
            show_header=True,
            header_style="bold magenta",
            box=None,
            padding=(0, 1),
        )
        tb.add_column("#",      width=2,  no_wrap=True)
        tb.add_column("",       width=1,  no_wrap=True)
        tb.add_column("Symbol", width=12, no_wrap=True)
        tb.add_column("24h%",   width=7,  no_wrap=True)
        if not narrow:
            tb.add_column("VolX",  width=6, no_wrap=True)
            tb.add_column("ATR%",  width=6, no_wrap=True)
        tb.add_column("Score",  width=8,  no_wrap=True)

        for i, r in enumerate(self.scan):
            sel_mark = "[green]\u25a0[/]" if r.symbol in self.selected else "[dim]\u00b7[/]"
            pct_str  = (
                f"[red]{r.pct_24h:+.1f}%[/]"
                if r.pct_24h < 0
                else f"[green]{r.pct_24h:+.1f}%[/]"
            )
            score_bar = _score_bar(r.score, 100, width=8)
            if narrow:
                tb.add_row(
                    f"[bold]{i + 1}[/]", sel_mark, r.symbol,
                    pct_str, score_bar,
                )
            else:
                tb.add_row(
                    f"[bold]{i + 1}[/]", sel_mark, r.symbol,
                    pct_str,
                    f"{r.vol_ratio:.1f}x",
                    f"{r.atr_pct:.2f}%",
                    score_bar,
                )

        if not self.scan:
            tb.add_row("[dim]press r to scan[/]", "", "", "", "", "", "")

        sub = (
            f"updated {int(time.time() - self.scan_ts)}s ago"
            if self.scan_ts else "not yet scanned"
        )
        return Panel(
            tb,
            title="VOLATILITY SCANNER  (short-bias ranked)",
            subtitle=sub,
            style="magenta",
            padding=(0, 1),
        )

    def _render_selected_bar(self) -> Panel:
        """Render the selected-symbols bar."""
        parts: List[Text] = []
        for sym in sorted(self.selected):
            s     = self.setups.get(sym)
            style = _STATE_STYLE.get(s.state if s else "NO_SETUP", "dim")
            parts.append(Text(sym, style=f"bold {style}"))
        txt   = Text("  ").join(parts) if parts else Text("none", style="dim")
        count = Text(f"{len(self.selected)}/{MAX_SELECT}  ", style="bold")
        count.append_text(txt)
        return Panel(count, title="SELECTED", style="blue", padding=(0, 1))

    def _render_coin_row(self, sym: str, narrow: bool) -> Panel:
        """Render a compact per-coin monitoring row."""
        s    = self.setups.get(sym) or Setup()
        a    = s.last
        tick = self.ticks.get(sym) or TickData()
        fi   = self.funding_oi.get(sym) or FundingOI()

        tb = Table.grid(padding=(0, 1))
        tb.add_column(min_width=16)
        tb.add_column()

        # Live price row
        px_color  = _px_color(tick)
        px_str    = f"{tick.price:.6g}" if tick.price else "-"
        chg_str   = (
            f"{tick.price_change_1s:+.4g}"
            if tick.price_change_1s != 0 else ""
        )
        spark     = _sparkline(tick.price_hist)
        countdown = _candle_close_countdown()
        tb.add_row(
            "Live price",
            f"[bold {px_color}]{_px_arrow(tick)} {px_str}[/]  [{px_color}]{chg_str}[/]  "
            f"[dim]{spark}[/]  {_tick_age_str(tick)}  [dim]close in {countdown}s[/]",
        )

        if not narrow:
            spread_str = (
                f"{tick.spread:.4g} ({tick.spread_pct:.3f}%)"
                if tick.spread else "-"
            )
            cvd_color = "red" if tick.cvd_delta < 0 else "green"
            tb.add_row("Bid/Ask spread", spread_str)
            tb.add_row(
                "CVD (60s)",
                f"[{cvd_color}]{tick.cvd_delta:+.2f}[/]",
            )
            fr_color = "red" if fi.funding_rate > 0 else "green"
            tb.add_row(
                "Funding rate",
                f"[{fr_color}]{fi.funding_rate:+.4f}%[/]",
            )
            oi_color = "red" if fi.oi_change_pct < 0 else "green"
            tb.add_row(
                "OI change",
                f"[{oi_color}]{fi.oi_change_pct:+.2f}%[/]",
            )

        # SMC conditions (compact, weighted)
        if a:
            for k in WEIGHTS:
                st    = a.conds[k]
                mark  = _COND_MARK[st]
                label = COND_LABELS[k]
                if st == CondState.OK:
                    pts = f"[green]+{WEIGHTS[k]}[/]"
                elif st == CondState.DEV:
                    pts = f"[yellow]~{WEIGHTS[k]}[/]"
                else:
                    pts = f"[dim]\u00b7{WEIGHTS[k]}[/]"
                tb.add_row(label, f"{mark} {pts}")
            tb.add_row("[bold]-- ENGINE SCORE --[/]", "")
            tb.add_row("Score",      _score_gauge(a.score))
            tb.add_row("Next state", _next_state_info(a.score))
            tb.add_row(
                "Thresholds",
                f"[cyan]WATCH {SCORE_WATCH}[/] [dim]/[/] "
                f"[yellow]PRE {SCORE_PRE}[/] [dim]/[/] "
                f"[red]ARMED {SCORE_ARMED}[/]",
            )
            tb.add_row("HTF bias",   a.htf_bias)
            tb.add_row("Zone type",  a.premium_disc)
            tb.add_row(
                "Confidence",
                Text(s.confidence.value, style=_CONF_STYLE[s.confidence]),
            )
        else:
            tb.add_row(f"[dim]{_spinner()} waiting for closed candles...[/]", "")

        # LuxAlgo-style 1m detections
        d = s.detections
        if d:
            tb.add_row("[bold magenta]-- SMC 1m --[/]", "")
            tb.add_row(
                "EQH",
                f"[red]\u2713 {d.eqh_level:.6g}[/]"
                if d.eqh_level is not None else "[dim]\u25cb[/]",
            )
            tb.add_row(
                "EQL",
                f"[green]\u2713 {d.eql_level:.6g}[/]"
                if d.eql_level is not None else "[dim]\u25cb[/]",
            )
            bear_z = d.bear_obs[-1] if d.bear_obs else None
            tb.add_row(
                "Bear OB",
                f"[red]\u2713 {bear_z.bottom:.6g}-{bear_z.top:.6g} "
                f"(x{len(d.bear_obs)})[/]"
                if bear_z else "[dim]\u25cb[/]",
            )
            bull_z = d.bull_obs[-1] if d.bull_obs else None
            tb.add_row(
                "Bull OB",
                f"[green]\u2713 {bull_z.bottom:.6g}-{bull_z.top:.6g} "
                f"(x{len(d.bull_obs)})[/]"
                if bull_z else "[dim]\u25cb[/]",
            )
            if d.any_detected:
                tb.add_row(
                    "",
                    Text("\U0001f514 PRE-TRADE ACTIVE (1m)", style="bold magenta"),
                )

        banner_text  = _STATE_ICON.get(s.state, s.state)
        banner_style = _STATE_STYLE.get(s.state, "dim")
        tb.add_row("", Text(banner_text, style=banner_style))

        panel_style = (
            "bold yellow" if s.state == "PRE_SIGNAL"
            else "bold red" if s.state == "ARMED"
            else "white"
        )
        return Panel(
            tb,
            title=f"[bold]{sym}[/]  {px_str}  1M",
            style=panel_style,
            padding=(0, 1),
        )

    def _render_detail(self, sym: str) -> Panel:
        """Render a full-detail view for one symbol."""
        s    = self.setups.get(sym) or Setup()
        a    = s.last
        tick = self.ticks.get(sym) or TickData()
        fi   = self.funding_oi.get(sym) or FundingOI()

        tb = Table.grid(padding=(0, 1))
        tb.add_column(min_width=20)
        tb.add_column()

        # Live data section
        px_color = _px_color(tick)
        tb.add_row("[bold]-- LIVE DATA --[/]", "")
        tb.add_row(
            "Price",
            f"[bold {px_color}]{_px_arrow(tick)} {tick.price:.8g}[/]  "
            f"[{px_color}]{tick.price_change_1s:+.4g}[/]  {_tick_age_str(tick)}",
        )
        tb.add_row("Sparkline", f"[dim]{_sparkline(tick.price_hist)}[/]")
        tb.add_row(
            "Bid / Ask",
            f"{tick.bid:.8g} / {tick.ask:.8g}  "
            f"spread {tick.spread:.4g} ({tick.spread_pct:.3f}%)",
        )
        cvd_color = "red" if tick.cvd_delta < 0 else "green"
        tb.add_row("CVD 60s", f"[{cvd_color}]{tick.cvd_delta:+.4f}[/]")
        fr_color = "red" if fi.funding_rate > 0 else "green"
        tb.add_row("Funding rate", f"[{fr_color}]{fi.funding_rate:+.4f}%[/]")
        oi_color = "red" if fi.oi_change_pct < 0 else "green"
        tb.add_row("OI change", f"[{oi_color}]{fi.oi_change_pct:+.2f}%[/]")
        tb.add_row("Candle closes in", f"{_candle_close_countdown()}s")

        if a:
            tb.add_row("[bold]-- SMC CONDITIONS (weighted) --[/]", "")
            for k in WEIGHTS:
                st   = a.conds[k]
                mark = _COND_MARK[st]
                if st == CondState.OK:
                    pts = f"[green]+{WEIGHTS[k]} pts[/]"
                elif st == CondState.DEV:
                    pts = f"[yellow]~{WEIGHTS[k]} pts (developing)[/]"
                else:
                    pts = f"[dim]0/{WEIGHTS[k]} pts[/]"
                tb.add_row(COND_LABELS[k], f"{mark} {pts}")

            tb.add_row("[bold]-- ENGINE SCORE --[/]", "")
            tb.add_row("Score",       _score_gauge(a.score, width=24))
            tb.add_row("Next state",  _next_state_info(a.score))
            tb.add_row(
                "Thresholds",
                f"[cyan]WATCH \u2265{SCORE_WATCH}[/]  "
                f"[yellow]PRE_SIGNAL \u2265{SCORE_PRE}[/]  "
                f"[red]ARMED \u2265{SCORE_ARMED}[/]",
            )
            tb.add_row(
                "Confidence",
                Text(a.confidence.value, style=_CONF_STYLE[a.confidence]),
            )
            tb.add_row("HTF bias",    a.htf_bias)
            tb.add_row("Zone type",   a.premium_disc)
            tb.add_row("Session",     a.session)
            tb.add_row("ATR (1m)",    f"{a.atr:.6g}")
            if a.zone[0]:
                tb.add_row(
                    "Short zone",
                    f"{a.zone[0]:.8g} - {a.zone[1]:.8g}",
                )
            if a.invalidation:
                tb.add_row("Invalidation", f"{a.invalidation:.8g}")
            tb.add_row("Setup age",   _age_str(s.born))
            tb.add_row("Bar count",   str(s.bar_count))
            tb.add_row("Reasons",     "\n".join(a.reasons) or "-")

        # LuxAlgo-style 1m detections (full listing)
        d = s.detections
        if d:
            tb.add_row("[bold magenta]-- SMC 1m DETECTIONS --[/]", "")
            tb.add_row(
                "Equal Highs (EQH)",
                f"[red]\u2713 {d.eqh_level:.8g}[/]"
                if d.eqh_level is not None else "[dim]none[/]",
            )
            tb.add_row(
                "Equal Lows (EQL)",
                f"[green]\u2713 {d.eql_level:.8g}[/]"
                if d.eql_level is not None else "[dim]none[/]",
            )
            if d.bear_obs:
                zones = "\n".join(
                    f"[red]{z.bottom:.8g} - {z.top:.8g}[/]"
                    for z in reversed(d.bear_obs)
                )
                tb.add_row(f"Bearish OB x{len(d.bear_obs)}", zones)
            else:
                tb.add_row("Bearish OB", "[dim]none[/]")
            if d.bull_obs:
                zones = "\n".join(
                    f"[green]{z.bottom:.8g} - {z.top:.8g}[/]"
                    for z in reversed(d.bull_obs)
                )
                tb.add_row(f"Bullish OB x{len(d.bull_obs)}", zones)
            else:
                tb.add_row("Bullish OB", "[dim]none[/]")
            if d.any_detected:
                tb.add_row(
                    "",
                    Text("\U0001f514 PRE-TRADE ACTIVE (1m)", style="bold magenta"),
                )

        banner_text  = _STATE_ICON.get(s.state, s.state)
        banner_style = _STATE_STYLE.get(s.state, "dim")
        tb.add_row("", Text(banner_text, style=f"bold {banner_style}"))

        panel_style = (
            "bold yellow" if s.state == "PRE_SIGNAL"
            else "bold red" if s.state == "ARMED"
            else "white"
        )
        return Panel(
            tb,
            title=f"[bold]DETAIL: {sym}[/]  {tick.price:.8g}  1M",
            style=panel_style,
            padding=(0, 1),
        )

    def _render_alert_feed(self) -> Panel:
        """Render the alert history feed panel."""
        tb = Table(
            show_header=True,
            header_style="bold yellow",
            box=None,
            padding=(0, 1),
        )
        tb.add_column("Time",   width=10, no_wrap=True)
        tb.add_column("Symbol", width=12, no_wrap=True)
        tb.add_column("State",  width=14, no_wrap=True)
        tb.add_column("Score",  width=8,  no_wrap=True)
        tb.add_column("Conf",   width=6,  no_wrap=True)

        db_alerts = self.store.recent_alerts(ALERT_FEED_MAX)
        for row in db_alerts:
            ts_str = datetime.fromtimestamp(
                row["ts"], tz=timezone.utc
            ).strftime("%H:%M:%S")
            state  = row["state"]
            style  = _STATE_STYLE.get(state, "dim")
            score  = f"{row['score']}/{row['max_score']}"
            conf   = row.get("confidence", "-")
            tb.add_row(
                ts_str,
                row["symbol"],
                Text(state, style=style),
                score,
                conf,
            )

        if not db_alerts:
            tb.add_row("[dim]no alerts yet[/]", "", "", "", "")

        return Panel(
            tb,
            title="ALERT FEED  (press f to toggle)",
            style="yellow",
            padding=(0, 1),
        )

    def _render_footer(self, narrow: bool) -> Panel:
        """Render the footer with keyboard shortcuts and disclaimer."""
        if narrow:
            keys = (
                "[bold]1-9[/] sel  [bold]a[/] auto  "
                "[bold]s[/] start  [bold]x[/] stop  [bold]q[/] quit"
            )
        else:
            keys = (
                "[bold]1-9[/] select  [bold]a[/] auto-select  "
                "[bold]s[/] start  [bold]x[/] stop  "
                "[bold]r[/] rescan  [bold]d[/] detail  "
                "[bold]f[/] feed  [bold]q[/] quit"
            )
        disclaimer = (
            "[dim]analysis only - NOT financial advice - "
            "\U0001f7e1 POSSIBLE FUTURE SHORT - "
            "\U0001f514 PRE-TRADE = 1m EQH/EQL/OB detection[/]"
        )
        return Panel(
            Text.from_markup(
                f"{keys}\n{disclaimer}\n[dim]{self.status_msg}[/]"
            ),
            style="cyan",
            padding=(0, 1),
        )


# ===========================================================================
# ENTRY POINT
# ===========================================================================


async def _amain() -> None:
    """Async main coroutine."""
    await App().run()


def main() -> None:
    """CLI entry point with safe terminal restore on exit."""
    try:
        asyncio.run(_amain())
    except KeyboardInterrupt:
        pass
    finally:
        with contextlib.suppress(Exception):
            import termios as _t
            fd = sys.stdin.fileno()
            _t.tcsetattr(fd, _t.TCSADRAIN, _t.tcgetattr(fd))
        print("\nSMC engine stopped.")


if __name__ == "__main__":
    main()
