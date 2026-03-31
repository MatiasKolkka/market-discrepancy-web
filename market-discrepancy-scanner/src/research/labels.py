from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
import math
import re
from typing import Any

import pandas as pd

from research.event_parser import parse_event
from research.features import download_price_history


@dataclass(frozen=True)
class SettledOutcome:
    ticker: str
    event_name: str
    settled_at: pd.Timestamp
    yes_outcome: int


def _to_timestamp(value: Any) -> pd.Timestamp | None:
    if value is None:
        return None
    ts = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(ts):
        return None
    return ts


def _to_yes_outcome(value: Any) -> int | None:
    if value is None:
        return None

    if isinstance(value, bool):
        return 1 if value else 0

    if isinstance(value, (int, float)):
        v = float(value)
        if 0.0 <= v <= 1.0:
            return 1 if v >= 0.5 else 0
        if 0.0 <= v <= 100.0:
            return 1 if (v / 100.0) >= 0.5 else 0
        return None

    text = str(value).strip().lower()
    if text in {"yes", "y", "true", "1", "win", "won", "up"}:
        return 1
    if text in {"no", "n", "false", "0", "lose", "lost", "down"}:
        return 0

    return None


def normalize_settled_markets(markets: list[dict[str, Any]]) -> list[SettledOutcome]:
    rows: list[SettledOutcome] = []

    for item in markets:
        ticker = str(item.get("ticker") or item.get("market_ticker") or "")
        event_name = str(item.get("title") or item.get("event_title") or ticker)

        outcome_raw = (
            item.get("result")
            or item.get("outcome")
            or item.get("winner")
            or item.get("settlement_value")
            or item.get("yes_settle")
            or item.get("yesSettlementPrice")
        )
        yes_outcome = _to_yes_outcome(outcome_raw)
        if yes_outcome is None:
            continue

        settled_ts = (
            _to_timestamp(item.get("settled_time"))
            or _to_timestamp(item.get("settledAt"))
            or _to_timestamp(item.get("close_time"))
            or _to_timestamp(item.get("closeTime"))
            or _to_timestamp(item.get("expiration_time"))
            or _to_timestamp(item.get("expirationTime"))
            or _to_timestamp(item.get("end_date"))
            or _to_timestamp(item.get("expires_at"))
        )
        if settled_ts is None:
            continue

        rows.append(
            SettledOutcome(
                ticker=ticker,
                event_name=event_name,
                settled_at=settled_ts,
                yes_outcome=yes_outcome,
            )
        )

    return rows


def _build_symbol_feature_table(symbol: str, lookback_days: int) -> pd.DataFrame:
    prices = download_price_history(symbol, lookback_days).copy()
    prices["ret"] = prices["close"].pct_change()
    prices["rv_20"] = prices["ret"].rolling(20).std() * (252.0 ** 0.5)
    prices["rv_60"] = prices["ret"].rolling(60).std() * (252.0 ** 0.5)
    prices["momentum_5d"] = prices["close"].pct_change(5)
    prices["momentum_20d"] = prices["close"].pct_change(20)

    table = prices.dropna().copy()
    table["timestamp"] = pd.to_datetime(table.index, utc=True)
    table.set_index("timestamp", inplace=True)
    return table


def build_real_outcome_dataset(
    settled_markets: list[dict[str, Any]],
    default_symbol: str,
    lookback_days: int,
) -> pd.DataFrame:
    normalized = normalize_settled_markets(settled_markets)
    if not normalized:
        return pd.DataFrame()

    candidates: list[SettledOutcome] = []
    for item in normalized:
        text = (item.event_name or "").upper()
        explicit_symbol = bool(re.search(r"\b(SPY|QQQ|IWM|DIA|NDX|SPX)\b", text))
        has_price_language = bool(re.search(r"\b(CLOSE|PRICE|SETTLE|SETTLED|FINISH|END)\b", text))
        has_threshold = bool(re.search(r"(?:ABOVE|OVER|BELOW|UNDER|>|<)\s*\d+(?:\.\d+)?", text))
        if explicit_symbol and has_price_language and has_threshold:
            candidates.append(item)

    if not candidates:
        return pd.DataFrame()

    symbols = {
        parse_event(item.event_name, default_symbol=default_symbol).symbol
        for item in candidates
    }

    tables: dict[str, pd.DataFrame] = {}
    for symbol in symbols:
        try:
            tables[symbol] = _build_symbol_feature_table(symbol, lookback_days)
        except Exception:
            continue

    rows: list[dict[str, Any]] = []
    for item in candidates:
        parsed = parse_event(item.event_name, default_symbol=default_symbol)
        if parsed.strike is None:
            continue

        table = tables.get(parsed.symbol)
        if table is None or table.empty:
            continue

        anchor_time = item.settled_at - timedelta(days=max(1, parsed.days_to_expiry))
        history = table.loc[table.index <= anchor_time]
        if history.empty:
            continue

        base = history.iloc[-1]
        spot = float(base["close"])
        strike = float(parsed.strike)

        rows.append(
            {
                "timestamp": anchor_time,
                "symbol": parsed.symbol,
                "ticker": item.ticker,
                "event_name": item.event_name,
                "spot": spot,
                "strike": strike,
                "days_to_expiry": int(parsed.days_to_expiry),
                "moneyness": float(math.log(strike / spot)),
                "rv_20": float(base["rv_20"]),
                "rv_60": float(base["rv_60"]),
                "momentum_5d": float(base["momentum_5d"]),
                "momentum_20d": float(base["momentum_20d"]),
                "label": int(item.yes_outcome if parsed.direction != "below" else 1 - item.yes_outcome),
                "label_source": "settled_market",
                "settled_at": item.settled_at,
            }
        )

    if not rows:
        return pd.DataFrame()

    return pd.DataFrame(rows)
