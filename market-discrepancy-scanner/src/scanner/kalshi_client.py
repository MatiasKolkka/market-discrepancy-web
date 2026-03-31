from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import requests

from scanner.models import MarketSignal


class KalshiClient:
    def __init__(self, api_base: str, api_key: str | None = None) -> None:
        self.api_base = api_base.rstrip("/")
        self.api_key = api_key

    def _headers(self) -> dict[str, str]:
        headers = {"Accept": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        return headers

    def _fetch_markets_with_params(self, params: dict[str, Any]) -> list[dict[str, Any]]:
        url = f"{self.api_base}/trade-api/v2/markets"
        response = requests.get(url, headers=self._headers(), params=params, timeout=15)
        response.raise_for_status()
        payload = response.json()

        if isinstance(payload, dict) and "markets" in payload and isinstance(payload["markets"], list):
            return payload["markets"]
        if isinstance(payload, list):
            return payload
        return []

    def fetch_markets(self, limit: int = 25) -> list[dict[str, Any]]:
        # NOTE: Endpoint shape can change. Keep parser defensive.
        return self._fetch_markets_with_params({"limit": limit})

    def fetch_markets_with_fallback(
        self,
        limit: int = 50,
        symbol_hint: str | None = None,
        symbol_hints: list[str] | None = None,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        candidates, deduped_hints = self._build_market_query_candidates(
            limit=limit,
            symbol_hint=symbol_hint,
            symbol_hints=symbol_hints,
        )

        attempts: list[dict[str, Any]] = []
        last_error: Exception | None = None

        for params in candidates:
            try:
                rows = self._fetch_markets_with_params(params)
                attempts.append({"params": params, "count": len(rows), "error": None})
                if rows:
                    selected_hint = None
                    for key in ("search", "query", "title", "event_ticker", "ticker", "underlying"):
                        if key in params:
                            selected_hint = params[key]
                            break
                    return rows, {
                        "attempts": attempts,
                        "selected_params": params,
                        "selected_hint": selected_hint,
                        "requested_hints": deduped_hints,
                    }
            except Exception as exc:
                last_error = exc
                attempts.append({"params": params, "count": 0, "error": f"{type(exc).__name__}: {exc}"})

        if last_error is not None:
            raise last_error
        return [], {
            "attempts": attempts,
            "selected_params": None,
            "selected_hint": None,
            "requested_hints": deduped_hints,
        }

    def fetch_markets_source_cascade(
        self,
        limit: int = 50,
        symbol_hint: str | None = None,
        symbol_hints: list[str] | None = None,
        max_attempts: int = 40,
    ) -> list[dict[str, Any]]:
        candidates, _ = self._build_market_query_candidates(
            limit=limit,
            symbol_hint=symbol_hint,
            symbol_hints=symbol_hints,
        )

        out: list[dict[str, Any]] = []
        for params in candidates[: max(1, max_attempts)]:
            try:
                rows = self._fetch_markets_with_params(params)
                out.append(
                    {
                        "params": params,
                        "rows": rows,
                        "count": len(rows),
                        "error": None,
                    }
                )
            except Exception as exc:
                out.append(
                    {
                        "params": params,
                        "rows": [],
                        "count": 0,
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                )
        return out

    @staticmethod
    def _build_market_query_candidates(
        limit: int,
        symbol_hint: str | None = None,
        symbol_hints: list[str] | None = None,
    ) -> tuple[list[dict[str, Any]], list[str]]:
        hints: list[str] = []
        if symbol_hints:
            hints.extend([str(h).strip().upper() for h in symbol_hints if str(h).strip()])
        if symbol_hint and symbol_hint.strip():
            hints.append(symbol_hint.strip().upper())

        # Preserve order while removing duplicates.
        deduped_hints: list[str] = []
        seen: set[str] = set()
        for hint in hints:
            if hint in seen:
                continue
            seen.add(hint)
            deduped_hints.append(hint)

        candidates = [
            {"limit": limit},
            {"limit": limit, "status": "open"},
            {"limit": limit, "status": "active"},
            {"limit": limit, "event_status": "open"},
            {"limit": limit, "event_status": "active"},
        ]

        hint_candidates: list[dict[str, Any]] = []
        for hint in deduped_hints:
            hint_candidates.extend(
                [
                    {"limit": limit, "search": hint},
                    {"limit": limit, "query": hint},
                    {"limit": limit, "title": hint},
                    {"limit": limit, "event_ticker": hint},
                    {"limit": limit, "ticker": hint},
                    {"limit": limit, "underlying": hint},
                ]
            )
        if hint_candidates:
            candidates = [*hint_candidates, *candidates]
        return candidates, deduped_hints

    def fetch_settled_markets(self, limit: int = 200) -> list[dict[str, Any]]:
        # Endpoint/status values differ by API version, so we try common variants.
        candidates = [
            {"limit": limit, "status": "settled"},
            {"limit": limit, "status": "resolved"},
            {"limit": limit, "status": "closed"},
            {"limit": limit, "event_status": "settled"},
        ]

        last_error: Exception | None = None
        for params in candidates:
            try:
                rows = self._fetch_markets_with_params(params)
                if rows:
                    return rows
            except Exception as exc:
                last_error = exc

        if last_error is not None:
            raise last_error
        return []

    @staticmethod
    def save_markets_to_file(markets: list[dict[str, Any]], file_path: str) -> None:
        target = Path(file_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(markets, ensure_ascii=True, indent=2), encoding="utf-8")

    @staticmethod
    def load_markets_from_file(file_path: str) -> list[dict[str, Any]]:
        source = Path(file_path)
        if not source.exists():
            raise FileNotFoundError(f"Market replay file not found: {source}")

        payload = json.loads(source.read_text(encoding="utf-8"))
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
        if isinstance(payload, dict) and isinstance(payload.get("markets"), list):
            return [item for item in payload["markets"] if isinstance(item, dict)]
        raise ValueError("Replay file must contain a list of markets or an object with a 'markets' array")

    @staticmethod
    def to_signal_candidates(markets: list[dict[str, Any]]) -> list[MarketSignal]:
        candidates: list[MarketSignal] = []

        def _parse_probability(value: Any, *, already_probability: bool = False) -> float | None:
            if value is None:
                return None
            try:
                raw = float(value)
            except (TypeError, ValueError):
                return None

            if already_probability:
                prob = raw
            else:
                # Some payloads use cents (e.g. 48), some use dollars/probability (e.g. 0.48).
                prob = raw / 100.0 if raw > 1.0 else raw

            if 0.0 <= prob <= 1.0:
                return prob
            return None

        for item in markets:
            ticker = str(item.get("ticker") or item.get("market_ticker") or "UNKNOWN")
            event_name = str(item.get("title") or item.get("event_title") or ticker)

            yes_bid = (
                item.get("yes_bid")
                or item.get("yesBid")
                or item.get("yes_bid_price")
                or item.get("yes_bid_dollars")
            )
            yes_ask = (
                item.get("yes_ask")
                or item.get("yesAsk")
                or item.get("yes_price")
                or item.get("yes_ask_dollars")
            )

            yes_bid_prob = _parse_probability(
                yes_bid,
                already_probability=("yes_bid_dollars" in item),
            )
            yes_ask_prob = _parse_probability(
                yes_ask,
                already_probability=("yes_ask_dollars" in item),
            )
            yes_price = yes_ask_prob if yes_ask_prob is not None else yes_bid_prob
            if yes_price is None:
                continue

            market_probability = yes_price

            volume_raw = item.get("volume") or item.get("volume_24h") or item.get("volume24h") or item.get("volume_fp")
            open_interest_raw = item.get("open_interest") or item.get("openInterest") or item.get("open_interest_fp")

            # Placeholder model probability, replaced later by stock model.
            bid_prob = yes_bid_prob
            ask_prob = yes_ask_prob

            volume = None
            if volume_raw is not None:
                try:
                    volume = int(float(volume_raw))
                except (TypeError, ValueError):
                    volume = None

            open_interest = None
            if open_interest_raw is not None:
                try:
                    open_interest = int(float(open_interest_raw))
                except (TypeError, ValueError):
                    open_interest = None

            candidates.append(
                MarketSignal(
                    ticker=ticker,
                    event_name=event_name,
                    market_probability=market_probability,
                    model_probability=market_probability,
                    yes_bid=bid_prob,
                    yes_ask=ask_prob,
                    volume=volume,
                    open_interest=open_interest,
                )
            )

        return candidates
