from __future__ import annotations

import math
from dataclasses import dataclass

import pandas as pd
import yfinance as yf


@dataclass(frozen=True)
class LiveFeatures:
    symbol: str
    spot: float
    strike: float
    days_to_expiry: int
    moneyness: float
    rv_20: float
    rv_60: float
    momentum_5d: float
    momentum_20d: float


def _realized_vol(returns: pd.Series, window: int) -> pd.Series:
    return returns.rolling(window).std() * math.sqrt(252.0)


def download_price_history(symbol: str, lookback_days: int) -> pd.DataFrame:
    hist = yf.Ticker(symbol).history(period=f"{lookback_days}d")
    if hist.empty:
        raise ValueError(f"No price history returned for {symbol}")
    closes = hist[["Close"]].copy()
    closes.rename(columns={"Close": "close"}, inplace=True)
    return closes.dropna()


def make_live_features(symbol: str, strike: float, days_to_expiry: int, lookback_days: int = 500) -> LiveFeatures:
    prices = download_price_history(symbol, lookback_days)
    prices["ret"] = prices["close"].pct_change()
    prices["rv_20"] = _realized_vol(prices["ret"], 20)
    prices["rv_60"] = _realized_vol(prices["ret"], 60)
    prices["mom_5"] = prices["close"].pct_change(5)
    prices["mom_20"] = prices["close"].pct_change(20)

    latest = prices.dropna().iloc[-1]
    spot = float(latest["close"])

    return LiveFeatures(
        symbol=symbol,
        spot=spot,
        strike=float(strike),
        days_to_expiry=int(days_to_expiry),
        moneyness=float(math.log(strike / spot)),
        rv_20=float(latest["rv_20"]),
        rv_60=float(latest["rv_60"]),
        momentum_5d=float(latest["mom_5"]),
        momentum_20d=float(latest["mom_20"]),
    )


def build_training_dataset(symbol: str, lookback_days: int = 1500) -> pd.DataFrame:
    prices = download_price_history(symbol, lookback_days)
    prices["ret"] = prices["close"].pct_change()
    prices["rv_20"] = _realized_vol(prices["ret"], 20)
    prices["rv_60"] = _realized_vol(prices["ret"], 60)
    prices["mom_5"] = prices["close"].pct_change(5)
    prices["mom_20"] = prices["close"].pct_change(20)

    horizons = [3, 5, 7, 10]
    strike_offsets = [-0.06, -0.03, -0.01, 0.00, 0.01, 0.03, 0.06]

    rows: list[dict[str, float | int | str]] = []

    clean = prices.dropna().copy()
    for idx in range(len(clean)):
        current_date = clean.index[idx]
        spot = float(clean.iloc[idx]["close"])

        for horizon in horizons:
            future_idx = idx + horizon
            if future_idx >= len(clean):
                continue

            future_close = float(clean.iloc[future_idx]["close"])
            for off in strike_offsets:
                strike = spot * (1.0 + off)
                label = 1 if future_close > strike else 0

                rows.append(
                    {
                        "timestamp": current_date,
                        "symbol": symbol,
                        "spot": spot,
                        "strike": strike,
                        "days_to_expiry": horizon,
                        "moneyness": math.log(strike / spot),
                        "rv_20": float(clean.iloc[idx]["rv_20"]),
                        "rv_60": float(clean.iloc[idx]["rv_60"]),
                        "momentum_5d": float(clean.iloc[idx]["mom_5"]),
                        "momentum_20d": float(clean.iloc[idx]["mom_20"]),
                        "label": label,
                    }
                )

    if not rows:
        raise ValueError("No training samples built")

    return pd.DataFrame(rows)


def feature_columns() -> list[str]:
    return ["moneyness", "days_to_expiry", "rv_20", "rv_60", "momentum_5d", "momentum_20d"]
