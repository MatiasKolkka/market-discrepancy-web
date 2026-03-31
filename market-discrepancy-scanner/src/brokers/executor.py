from __future__ import annotations

import json
import random
from datetime import datetime, timezone
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ExecutionDecision:
    execute: bool
    reason: str


class ExecutionGate:
    @staticmethod
    def check(enable_live_execution: bool, execution_mode: str) -> ExecutionDecision:
        mode = execution_mode.strip().lower()

        if mode not in {"paper", "live"}:
            return ExecutionDecision(False, f"Invalid EXECUTION_MODE={execution_mode!r}; expected 'paper' or 'live'.")

        if mode == "paper":
            return ExecutionDecision(False, "Paper mode active.")

        if not enable_live_execution:
            return ExecutionDecision(
                False,
                "Live mode requested but ENABLE_LIVE_EXECUTION is false. Blocking execution.",
            )

        return ExecutionDecision(True, "Live execution enabled.")


class BrokerExecutor:
    def place_order(self, market_ticker: str, side: str, quantity: int = 1) -> None:
        # Stub by design. Replace this with authenticated broker/Kalshi order placement.
        print(f"[EXECUTE] place_order ticker={market_ticker} side={side} qty={quantity}")


class PaperExecutor:
    def __init__(
        self,
        log_path: str,
        fill_probability: float = 0.95,
        partial_fill_min_ratio: float = 0.60,
        partial_fill_max_ratio: float = 1.00,
        slippage_std_probability: float = 0.01,
        delay_ms_min: int = 20,
        delay_ms_max: int = 250,
    ) -> None:
        self.log_path = Path(log_path)
        self.fill_probability = max(0.0, min(1.0, fill_probability))
        self.partial_fill_min_ratio = max(0.0, min(1.0, partial_fill_min_ratio))
        self.partial_fill_max_ratio = max(self.partial_fill_min_ratio, min(1.0, partial_fill_max_ratio))
        self.slippage_std_probability = max(0.0, slippage_std_probability)
        self.delay_ms_min = max(0, delay_ms_min)
        self.delay_ms_max = max(self.delay_ms_min, delay_ms_max)

    def place_order(
        self,
        market_ticker: str,
        side: str,
        quantity: int,
        market_probability: float,
        model_probability: float,
        net_edge: float,
        confidence: float,
    ) -> dict[str, float | int | bool]:
        self.log_path.parent.mkdir(parents=True, exist_ok=True)

        filled = random.random() <= self.fill_probability
        fill_ratio = random.uniform(self.partial_fill_min_ratio, self.partial_fill_max_ratio)
        filled_quantity = int(max(0, min(quantity, round(quantity * fill_ratio)))) if filled else 0

        slippage = random.gauss(0.0, self.slippage_std_probability)
        effective_probability = market_probability
        if side == "buy_yes":
            effective_probability = min(1.0, max(0.0, market_probability + slippage))
        else:
            effective_probability = min(1.0, max(0.0, market_probability - slippage))

        delay_ms = random.randint(self.delay_ms_min, self.delay_ms_max)

        row = {
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "ticker": market_ticker,
            "side": side,
            "quantity": quantity,
            "filled": filled,
            "filled_quantity": filled_quantity,
            "fill_ratio": fill_ratio,
            "effective_probability": effective_probability,
            "fill_delay_ms": delay_ms,
            "market_probability": market_probability,
            "model_probability": model_probability,
            "net_edge": net_edge,
            "confidence": confidence,
            "mode": "paper",
        }
        with self.log_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row) + "\n")

        print(
            "[PAPER]"
            f" ticker={market_ticker} side={side} qty={quantity}"
            f" filled_qty={filled_quantity}"
            f" eff_market={effective_probability:.3f}"
            f" model={model_probability:.3f} net_edge={net_edge:.3f}"
        )

        return {
            "filled": filled,
            "filled_quantity": filled_quantity,
            "effective_probability": effective_probability,
            "fill_delay_ms": delay_ms,
        }
