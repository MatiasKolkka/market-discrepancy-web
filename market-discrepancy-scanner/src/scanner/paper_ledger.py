from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from research.labels import normalize_settled_markets
from scanner.risk import PortfolioState


@dataclass
class PaperPosition:
    ticker: str
    side: str
    quantity: int
    entry_probability: float
    entry_cost_dollars: float
    opened_at_utc: str


class PaperLedger:
    def __init__(self, state_path: str, event_log_path: str) -> None:
        self.state_path = Path(state_path)
        self.event_log_path = Path(event_log_path)
        self.positions: list[PaperPosition] = []
        self._load_state()

    def _load_state(self) -> None:
        if not self.state_path.exists():
            self.positions = []
            return

        try:
            payload = json.loads(self.state_path.read_text(encoding="utf-8"))
        except Exception:
            self.positions = []
            return

        rows = payload.get("positions") if isinstance(payload, dict) else None
        if not isinstance(rows, list):
            self.positions = []
            return

        loaded: list[PaperPosition] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            try:
                loaded.append(
                    PaperPosition(
                        ticker=str(row.get("ticker")),
                        side=str(row.get("side")),
                        quantity=int(row.get("quantity")),
                        entry_probability=float(row.get("entry_probability")),
                        entry_cost_dollars=float(row.get("entry_cost_dollars")),
                        opened_at_utc=str(row.get("opened_at_utc")),
                    )
                )
            except (TypeError, ValueError):
                continue

        self.positions = loaded

    def _save_state(self) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "updated_at_utc": datetime.now(timezone.utc).isoformat(),
            "positions": [asdict(pos) for pos in self.positions],
        }
        self.state_path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")

    def _append_event(self, payload: dict[str, Any]) -> None:
        self.event_log_path.parent.mkdir(parents=True, exist_ok=True)
        row = {
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            **payload,
        }
        with self.event_log_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row) + "\n")

    def add_position(
        self,
        ticker: str,
        side: str,
        quantity: int,
        market_probability: float,
    ) -> None:
        if quantity <= 0:
            return

        entry_prob = market_probability if side == "buy_yes" else (1.0 - market_probability)
        entry_cost = max(0.0, entry_prob * quantity)

        self.positions.append(
            PaperPosition(
                ticker=ticker,
                side=side,
                quantity=quantity,
                entry_probability=entry_prob,
                entry_cost_dollars=entry_cost,
                opened_at_utc=datetime.now(timezone.utc).isoformat(),
            )
        )

        self._append_event(
            {
                "event_type": "paper_open",
                "ticker": ticker,
                "side": side,
                "quantity": quantity,
                "entry_probability": entry_prob,
                "entry_cost_dollars": entry_cost,
            }
        )
        self._save_state()

    def reconcile_settlements(
        self,
        settled_markets: list[dict[str, Any]],
        portfolio: PortfolioState,
    ) -> dict[str, float | int]:
        outcomes = {item.ticker: item.yes_outcome for item in normalize_settled_markets(settled_markets)}
        if not outcomes:
            return {"closed_positions": 0, "realized_pnl_dollars": 0.0}

        remaining: list[PaperPosition] = []
        closed = 0
        realized_pnl = 0.0

        for pos in self.positions:
            if pos.ticker not in outcomes:
                remaining.append(pos)
                continue

            yes_outcome = outcomes[pos.ticker]
            payout = 0.0
            if pos.side == "buy_yes":
                payout = float(pos.quantity if yes_outcome == 1 else 0)
            elif pos.side == "buy_no":
                payout = float(pos.quantity if yes_outcome == 0 else 0)

            pnl = payout - pos.entry_cost_dollars
            realized_pnl += pnl
            closed += 1

            portfolio.gross_exposure_dollars = max(0.0, portfolio.gross_exposure_dollars - pos.entry_cost_dollars)
            portfolio.used_cash_dollars = max(0.0, portfolio.used_cash_dollars - pos.entry_cost_dollars)
            portfolio.bankroll_dollars += pnl
            portfolio.day_realized_pnl_dollars += pnl

            current = portfolio.ticker_exposure_dollars.get(pos.ticker, 0.0)
            updated = max(0.0, current - pos.entry_cost_dollars)
            if updated <= 1e-9:
                portfolio.ticker_exposure_dollars.pop(pos.ticker, None)
            else:
                portfolio.ticker_exposure_dollars[pos.ticker] = updated

            self._append_event(
                {
                    "event_type": "paper_settlement",
                    "ticker": pos.ticker,
                    "side": pos.side,
                    "quantity": pos.quantity,
                    "entry_cost_dollars": pos.entry_cost_dollars,
                    "yes_outcome": yes_outcome,
                    "payout_dollars": payout,
                    "realized_pnl_dollars": pnl,
                }
            )

        self.positions = remaining
        self._save_state()
        return {
            "closed_positions": closed,
            "realized_pnl_dollars": round(realized_pnl, 6),
        }
