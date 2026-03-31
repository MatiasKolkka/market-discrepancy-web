from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path


@dataclass
class MarketDiagnostics:
    scanned: int = 0
    by_reason: dict[str, int] = field(default_factory=dict)
    examples_by_reason: dict[str, list[dict[str, str]]] = field(default_factory=dict)
    metadata: dict[str, object] = field(default_factory=dict)

    def add(self, reason: str, ticker: str, event_name: str, max_examples: int = 5) -> None:
        key = reason.strip() or "unknown"
        self.by_reason[key] = self.by_reason.get(key, 0) + 1

        bucket = self.examples_by_reason.setdefault(key, [])
        if len(bucket) < max_examples:
            bucket.append({"ticker": ticker, "event_name": event_name})

    def as_dict(self) -> dict[str, object]:
        pass_key = "Passes pre-score filters"
        passed = int(self.by_reason.get(pass_key, 0))
        blocked = sum(v for k, v in self.by_reason.items() if k != pass_key)
        return {
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "scanned": self.scanned,
            "blocked": blocked,
            "passed": passed,
            "by_reason": self.by_reason,
            "examples_by_reason": self.examples_by_reason,
            "metadata": self.metadata,
        }

    def save_json(self, file_path: str) -> None:
        target = Path(file_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(self.as_dict(), ensure_ascii=True, indent=2), encoding="utf-8")
