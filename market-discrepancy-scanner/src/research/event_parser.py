from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class ParsedEvent:
    symbol: str
    strike: float | None
    strike_upper: float | None
    direction: str
    days_to_expiry: int
    has_explicit_symbol: bool
    is_price_contract: bool


def parse_event(event_name: str, default_symbol: str = "SPY") -> ParsedEvent:
    text = (event_name or "").upper()

    symbol_match = re.search(r"\b(SPY|QQQ|IWM|DIA|NDX|SPX)\b", text)
    symbol = symbol_match.group(1) if symbol_match else default_symbol
    has_explicit_symbol = symbol_match is not None

    strike = None
    strike_upper = None
    direction = "above"

    range_match = re.search(
        r"(?:BETWEEN|FROM)\s*(\d+(?:\.\d+)?)\s*(?:AND|TO)\s*(\d+(?:\.\d+)?)",
        text,
    )
    if range_match:
        try:
            lower = float(range_match.group(1))
            upper = float(range_match.group(2))
            strike = min(lower, upper)
            strike_upper = max(lower, upper)
            direction = "range"
        except ValueError:
            strike = None
            strike_upper = None
    else:
        strike_match = re.search(r"(?:ABOVE|OVER|AT\s+OR\s+ABOVE|>|>=)\s*(\d+(?:\.\d+)?)", text)
        if strike_match:
            try:
                strike = float(strike_match.group(1))
                direction = "above"
            except ValueError:
                strike = None
        else:
            strike_match = re.search(r"(?:BELOW|UNDER|AT\s+OR\s+BELOW|<|<=)\s*(\d+(?:\.\d+)?)", text)
            if strike_match:
                try:
                    strike = float(strike_match.group(1))
                    direction = "below"
                except ValueError:
                    strike = None

    days = 7
    if "TODAY" in text:
        days = 1
    elif "TOMORROW" in text:
        days = 2
    elif "THIS WEEK" in text or "BY FRIDAY" in text:
        days = 5
    elif "THIS MONTH" in text:
        days = 21

    has_threshold = strike is not None
    has_price_language = bool(re.search(r"\b(CLOSE|PRICE|SETTLE|SETTLED|FINISH|END|EXPIR)\b", text))
    is_price_contract = has_explicit_symbol and has_threshold and has_price_language

    return ParsedEvent(
        symbol=symbol,
        strike=strike,
        strike_upper=strike_upper,
        direction=direction,
        days_to_expiry=days,
        has_explicit_symbol=has_explicit_symbol,
        is_price_contract=is_price_contract,
    )
