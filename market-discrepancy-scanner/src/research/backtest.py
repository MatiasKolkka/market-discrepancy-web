from __future__ import annotations

from dataclasses import dataclass

import pandas as pd
from sklearn.metrics import brier_score_loss, log_loss

from research.features import feature_columns
from research.modeling import ProbabilityModel


@dataclass(frozen=True)
class WalkForwardResult:
    folds: int
    mean_brier: float
    mean_log_loss: float


def walk_forward_validate(
    frame: pd.DataFrame,
    min_train_rows: int = 500,
    test_rows: int = 200,
) -> WalkForwardResult:
    cols = feature_columns()
    data = frame.dropna(subset=cols + ["label"]).sort_values("timestamp").reset_index(drop=True)
    if len(data) < min_train_rows + test_rows:
        raise ValueError("Not enough data for walk-forward validation")

    briers: list[float] = []
    losses: list[float] = []

    start = min_train_rows
    while start + test_rows <= len(data):
        train = data.iloc[:start]
        test = data.iloc[start : start + test_rows]

        model = ProbabilityModel()
        model.fit(train)

        x_test = test[cols]
        y_test = test["label"].astype(int)
        probs = model.predict_batch(x_test)

        briers.append(float(brier_score_loss(y_test, probs)))
        losses.append(float(log_loss(y_test, probs, labels=[0, 1])))

        start += test_rows

    return WalkForwardResult(
        folds=len(briers),
        mean_brier=float(sum(briers) / len(briers)),
        mean_log_loss=float(sum(losses) / len(losses)),
    )
