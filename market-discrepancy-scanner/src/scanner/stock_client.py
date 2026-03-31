from __future__ import annotations

import math
from dataclasses import dataclass

import yfinance as yf


@dataclass(frozen=True)
class StockSnapshot:
    symbol: str
    price: float
    annualized_vol: float


class StockModel:
    def get_snapshot(self, symbol: str = "SPY", history_days: int = 60) -> StockSnapshot:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period=f"{history_days}d")
        if hist.empty:
            raise ValueError(f"No history returned for {symbol}")

        closes = hist["Close"].dropna()
        if closes.empty:
            raise ValueError(f"No closing prices returned for {symbol}")

        returns = closes.pct_change().dropna()
        if returns.empty:
            raise ValueError(f"Insufficient return history for {symbol}")

        price = float(closes.iloc[-1])
        daily_vol = float(returns.std())
        annualized_vol = daily_vol * math.sqrt(252.0)

        return StockSnapshot(symbol=symbol, price=price, annualized_vol=annualized_vol)

    @staticmethod
    def naive_probability_above(snapshot: StockSnapshot, strike: float, days_to_expiry: int = 7) -> float:
        # Fast normal-approximation style estimate, suitable only as baseline.
        if days_to_expiry <= 0:
            return 1.0 if snapshot.price > strike else 0.0

        t = days_to_expiry / 365.0
        sigma_t = snapshot.annualized_vol * math.sqrt(t)
        if sigma_t <= 0:
            return 1.0 if snapshot.price > strike else 0.0

        z = (math.log(strike / snapshot.price)) / sigma_t

        # Approximation of N(z) via erf.
        cdf = 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))
        return max(0.0, min(1.0, 1.0 - cdf))
