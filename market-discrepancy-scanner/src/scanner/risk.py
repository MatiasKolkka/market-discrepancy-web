from __future__ import annotations

import time
from dataclasses import dataclass, field


@dataclass
class PortfolioState:
    bankroll_dollars: float
    gross_exposure_dollars: float = 0.0
    used_cash_dollars: float = 0.0
    day_realized_pnl_dollars: float = 0.0
    ticker_exposure_dollars: dict[str, float] = field(default_factory=dict)
    last_trade_unix_seconds: dict[str, float] = field(default_factory=dict)

    @property
    def available_cash_dollars(self) -> float:
        return max(0.0, self.bankroll_dollars - self.used_cash_dollars)


@dataclass(frozen=True)
class RiskDecision:
    allowed: bool
    reason: str
    quantity: int = 0
    expected_value_dollars: float = 0.0


class RiskManager:
    def __init__(
        self,
        min_confidence: float,
        max_exposure_dollars: float,
        max_ticker_exposure_dollars: float,
        max_open_tickers: int,
        max_trade_size_dollars: float,
        daily_loss_limit_dollars: float,
        kelly_fraction: float,
        per_ticker_cooldown_seconds: int,
        min_expected_value_dollars_per_contract: float,
    ) -> None:
        self.min_confidence = min_confidence
        self.max_exposure_dollars = max_exposure_dollars
        self.max_ticker_exposure_dollars = max_ticker_exposure_dollars
        self.max_open_tickers = max_open_tickers
        self.max_trade_size_dollars = max_trade_size_dollars
        self.daily_loss_limit_dollars = daily_loss_limit_dollars
        self.kelly_fraction = kelly_fraction
        self.per_ticker_cooldown_seconds = per_ticker_cooldown_seconds
        self.min_expected_value_dollars_per_contract = min_expected_value_dollars_per_contract

    def approve(
        self,
        state: PortfolioState,
        ticker: str,
        side: str,
        market_probability: float,
        model_probability: float,
        net_edge: float,
        confidence: float,
        now_unix_seconds: float | None = None,
    ) -> RiskDecision:
        now_ts = now_unix_seconds if now_unix_seconds is not None else time.time()

        if state.day_realized_pnl_dollars <= -abs(self.daily_loss_limit_dollars):
            return RiskDecision(False, "Daily loss limit breached")

        if confidence < self.min_confidence:
            return RiskDecision(False, f"Low confidence {confidence:.3f}")

        per_contract_ev = abs(net_edge)
        if per_contract_ev <= 0:
            return RiskDecision(False, "Net edge not positive after cost")

        if per_contract_ev < self.min_expected_value_dollars_per_contract:
            return RiskDecision(
                False,
                f"Expected value too small: {per_contract_ev:.4f}",
            )

        if state.gross_exposure_dollars >= self.max_exposure_dollars:
            return RiskDecision(False, "Gross exposure limit reached")

        ticker_exposure = state.ticker_exposure_dollars.get(ticker, 0.0)
        if ticker_exposure >= self.max_ticker_exposure_dollars:
            return RiskDecision(False, "Ticker exposure limit reached")

        if ticker not in state.ticker_exposure_dollars and len(state.ticker_exposure_dollars) >= self.max_open_tickers:
            return RiskDecision(False, "Max open tickers reached")

        last_trade_ts = state.last_trade_unix_seconds.get(ticker)
        if last_trade_ts is not None and (now_ts - last_trade_ts) < self.per_ticker_cooldown_seconds:
            return RiskDecision(False, "Ticker cooldown active")

        p = model_probability if side == "buy_yes" else 1.0 - model_probability
        q = 1.0 - p

        # Kalshi-like payoff approximation where a share costs approximately probability dollars.
        c = market_probability if side == "buy_yes" else (1.0 - market_probability)
        b = max(1e-6, (1.0 - c) / max(c, 1e-6))
        kelly_raw = (b * p - q) / b
        f = max(0.0, min(self.kelly_fraction, kelly_raw * self.kelly_fraction))

        budget = min(
            self.max_trade_size_dollars,
            self.max_exposure_dollars - state.gross_exposure_dollars,
            self.max_ticker_exposure_dollars - ticker_exposure,
            state.available_cash_dollars,
        )
        if budget <= 0:
            return RiskDecision(False, "No available trade budget")

        allocation = budget * f
        if allocation <= 0:
            return RiskDecision(False, "Kelly sizing allocated zero")

        quantity = int(allocation / max(c, 0.01))
        if quantity < 1:
            return RiskDecision(False, "Trade too small after sizing")

        expected_value_dollars = quantity * per_contract_ev
        return RiskDecision(
            True,
            "Approved",
            quantity=quantity,
            expected_value_dollars=expected_value_dollars,
        )

    def register_fill(
        self,
        state: PortfolioState,
        ticker: str,
        side: str,
        quantity: int,
        market_probability: float,
        now_unix_seconds: float | None = None,
    ) -> None:
        now_ts = now_unix_seconds if now_unix_seconds is not None else time.time()
        price = market_probability if side == "buy_yes" else (1.0 - market_probability)
        notional = max(0.0, price * quantity)

        state.gross_exposure_dollars += notional
        state.used_cash_dollars += notional
        state.ticker_exposure_dollars[ticker] = state.ticker_exposure_dollars.get(ticker, 0.0) + notional
        state.last_trade_unix_seconds[ticker] = now_ts
