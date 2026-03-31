from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
from sklearn.calibration import CalibratedClassifierCV
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, log_loss

from research.features import feature_columns


@dataclass(frozen=True)
class ModelMetrics:
    brier_score: float
    log_loss_score: float
    expected_calibration_error: float
    rows: int


def _expected_calibration_error(y_true: pd.Series, probs: np.ndarray, bins: int = 10) -> float:
    if len(y_true) == 0:
        return 0.0

    edges = np.linspace(0.0, 1.0, bins + 1)
    y = y_true.to_numpy(dtype=float)

    ece = 0.0
    n = len(y)
    for idx in range(bins):
        lo = edges[idx]
        hi = edges[idx + 1]
        mask = (probs >= lo) & (probs < hi if idx < bins - 1 else probs <= hi)
        count = int(mask.sum())
        if count == 0:
            continue

        acc = float(y[mask].mean())
        conf = float(probs[mask].mean())
        ece += (count / n) * abs(acc - conf)

    return float(ece)


class ProbabilityModel:
    def __init__(self) -> None:
        self._model = CalibratedClassifierCV(
            estimator=LogisticRegression(max_iter=800),
            method="isotonic",
            cv=3,
        )
        self.is_fitted = False

    def fit(self, frame: pd.DataFrame) -> ModelMetrics:
        cols = feature_columns()
        train = frame.dropna(subset=cols + ["label"]).copy()
        if train.empty:
            raise ValueError("Training frame is empty after dropna")

        x = train[cols]
        y = train["label"].astype(int)

        self._model.fit(x, y)
        self.is_fitted = True

        probs = self._model.predict_proba(x)[:, 1]
        return ModelMetrics(
            brier_score=float(brier_score_loss(y, probs)),
            log_loss_score=float(log_loss(y, probs, labels=[0, 1])),
            expected_calibration_error=_expected_calibration_error(y, probs),
            rows=int(len(train)),
        )

    def predict_probability(self, feature_row: dict[str, float | int]) -> tuple[float, float]:
        if not self.is_fitted:
            raise RuntimeError("Model is not fitted")

        cols = feature_columns()
        one = pd.DataFrame([{key: feature_row[key] for key in cols}])
        p = float(self._model.predict_proba(one)[:, 1][0])

        # Confidence grows with distance from 0.5.
        confidence = min(1.0, max(0.0, abs(p - 0.5) * 2.0))
        return p, confidence

    def predict_batch(self, frame: pd.DataFrame) -> np.ndarray:
        if not self.is_fitted:
            raise RuntimeError("Model is not fitted")
        cols = feature_columns()
        return self._model.predict_proba(frame[cols])[:, 1]

    def save(self, path: str, metadata: dict[str, Any] | None = None) -> None:
        if not self.is_fitted:
            raise RuntimeError("Cannot save unfitted model")

        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        joblib.dump(
            {
                "model": self._model,
                "metadata": metadata or {},
            },
            target,
        )

    @classmethod
    def load(cls, path: str) -> tuple["ProbabilityModel", dict[str, Any]]:
        payload = joblib.load(path)
        if not isinstance(payload, dict) or "model" not in payload:
            raise ValueError("Invalid model artifact")

        inst = cls()
        inst._model = payload["model"]
        inst.is_fitted = True
        metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
        return inst, metadata
