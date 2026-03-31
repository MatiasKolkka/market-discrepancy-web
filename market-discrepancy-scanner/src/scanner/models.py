from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class MarketSignal:
    ticker: str
    event_name: str
    market_probability: float
    model_probability: float
    calibrated_probability: float | None = None
    model_confidence: float | None = None
    yes_bid: float | None = None
    yes_ask: float | None = None
    volume: int | None = None
    open_interest: int | None = None
    cost_probability: float = 0.0

    @property
    def spread_probability(self) -> float | None:
        if self.yes_bid is None or self.yes_ask is None:
            return None
        return max(0.0, self.yes_ask - self.yes_bid)

    @property
    def edge(self) -> float:
        model_prob = self.calibrated_probability if self.calibrated_probability is not None else self.model_probability
        return model_prob - self.market_probability

    @property
    def net_edge(self) -> float:
        signed = self.edge
        residual = abs(signed) - self.cost_probability
        if residual <= 0:
            return 0.0
        return residual if signed >= 0 else -residual


@dataclass(frozen=True)
class ScanResult:
    signal: MarketSignal
    should_alert: bool
    should_execute: bool
    order_side: str | None = None
    order_quantity: int = 0
    expected_value_dollars: float = 0.0
    block_reason: str | None = None
