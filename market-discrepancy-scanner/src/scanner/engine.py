from __future__ import annotations

import json
import math
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import requests
from sklearn.metrics import brier_score_loss, log_loss

from brokers.executor import BrokerExecutor, ExecutionGate, PaperExecutor
from config import Settings
from research.backtest import WalkForwardResult, walk_forward_validate
from research.event_parser import parse_event
from research.features import build_training_dataset, make_live_features
from research.labels import build_real_outcome_dataset, normalize_settled_markets
from research.modeling import ModelMetrics, ProbabilityModel
from scanner.diagnostics import MarketDiagnostics
from scanner.kalshi_client import KalshiClient
from scanner.models import MarketSignal, ScanResult
from scanner.paper_ledger import PaperLedger
from scanner.risk import PortfolioState, RiskManager


@dataclass
class ModelRuntimeState:
    model: ProbabilityModel | None = None
    model_metrics: ModelMetrics | None = None
    walk_forward: WalkForwardResult | None = None
    training_rows: int = 0
    training_data_source: str = "unknown"
    scan_count: int = 0
    model_source: str = "none"


RUNTIME = ModelRuntimeState()
PORTFOLIO = PortfolioState(bankroll_dollars=5000.0)


def send_alert(webhook_url: str | None, message: str) -> None:
    print(f"[ALERT] {message}")
    if not webhook_url:
        return

    try:
        requests.post(webhook_url, json={"text": message}, timeout=5)
    except requests.RequestException as exc:
        print(f"[WARN] failed to send webhook alert: {exc}")


def append_log(log_file: Path, signal: MarketSignal) -> None:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    row = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "ticker": signal.ticker,
        "event_name": signal.event_name,
        "market_probability": signal.market_probability,
        "model_probability": signal.model_probability,
        "calibrated_probability": signal.calibrated_probability,
        "model_confidence": signal.model_confidence,
        "yes_bid": signal.yes_bid,
        "yes_ask": signal.yes_ask,
        "spread_probability": signal.spread_probability,
        "volume": signal.volume,
        "open_interest": signal.open_interest,
        "cost_probability": signal.cost_probability,
        "edge": signal.edge,
        "net_edge": signal.net_edge,
    }
    with log_file.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row) + "\n")


def _cost_probability(signal: MarketSignal, settings: Settings) -> float:
    spread_cost = 0.0
    if signal.yes_bid is not None and signal.yes_ask is not None:
        spread_cost = max(0.0, (signal.yes_ask - signal.yes_bid) / 2.0)
    return max(0.0, settings.fee_rate_probability + settings.slippage_probability + spread_cost)


def _risk_manager(settings: Settings) -> RiskManager:
    return RiskManager(
        min_confidence=settings.min_model_confidence,
        max_exposure_dollars=settings.max_exposure_dollars,
        max_ticker_exposure_dollars=settings.max_ticker_exposure_dollars,
        max_open_tickers=settings.max_open_tickers,
        max_trade_size_dollars=settings.max_trade_size_dollars,
        daily_loss_limit_dollars=settings.daily_loss_limit_dollars,
        kelly_fraction=settings.kelly_fraction,
        per_ticker_cooldown_seconds=settings.per_ticker_cooldown_seconds,
        min_expected_value_dollars_per_contract=settings.min_expected_value_dollars_per_contract,
    )


def _artifact_is_fresh(path: Path, max_age_minutes: int) -> bool:
    if not path.exists():
        return False

    age_seconds = datetime.now(timezone.utc).timestamp() - path.stat().st_mtime
    return age_seconds <= max(0, max_age_minutes) * 60


def _load_model_artifact(settings: Settings) -> bool:
    artifact = Path(settings.model_artifact_path)
    if not _artifact_is_fresh(artifact, settings.model_max_age_minutes):
        return False

    try:
        loaded, metadata = ProbabilityModel.load(str(artifact))
    except Exception as exc:
        print(f"[WARN] failed loading model artifact {artifact}: {exc}")
        return False

    RUNTIME.model = loaded
    RUNTIME.model_source = "artifact"

    metrics = metadata.get("metrics") if isinstance(metadata.get("metrics"), dict) else None
    if metrics:
        RUNTIME.model_metrics = ModelMetrics(
            brier_score=float(metrics.get("brier_score", 0.0)),
            log_loss_score=float(metrics.get("log_loss_score", 0.0)),
            expected_calibration_error=float(metrics.get("expected_calibration_error", 0.0)),
            rows=int(metrics.get("rows", 0)),
        )

    wf = metadata.get("walk_forward") if isinstance(metadata.get("walk_forward"), dict) else None
    if wf:
        RUNTIME.walk_forward = WalkForwardResult(
            folds=int(wf.get("folds", 0)),
            mean_brier=float(wf.get("mean_brier", 0.0)),
            mean_log_loss=float(wf.get("mean_log_loss", 0.0)),
        )

    if isinstance(metadata.get("training_rows"), int):
        RUNTIME.training_rows = int(metadata.get("training_rows", 0))
    if isinstance(metadata.get("training_data_source"), str):
        RUNTIME.training_data_source = str(metadata.get("training_data_source", "unknown"))

    print(f"[MODEL] loaded artifact from {artifact}")
    return True


def _save_model_artifact(settings: Settings) -> None:
    if RUNTIME.model is None or not RUNTIME.model.is_fitted:
        return

    metrics = RUNTIME.model_metrics
    wf = RUNTIME.walk_forward

    metadata = {
        "trained_at_utc": datetime.now(timezone.utc).isoformat(),
        "training_symbol": settings.training_symbol,
        "training_rows": RUNTIME.training_rows,
        "training_data_source": RUNTIME.training_data_source,
        "metrics": {
            "brier_score": metrics.brier_score if metrics else 0.0,
            "log_loss_score": metrics.log_loss_score if metrics else 0.0,
            "expected_calibration_error": metrics.expected_calibration_error if metrics else 0.0,
            "rows": metrics.rows if metrics else 0,
        },
        "walk_forward": {
            "folds": wf.folds if wf else 0,
            "mean_brier": wf.mean_brier if wf else 0.0,
            "mean_log_loss": wf.mean_log_loss if wf else 0.0,
        },
    }

    try:
        RUNTIME.model.save(settings.model_artifact_path, metadata)
        print(f"[MODEL] saved artifact to {settings.model_artifact_path}")
    except Exception as exc:
        print(f"[WARN] failed to save model artifact: {exc}")


def _choose_training_dataset(settings: Settings, kalshi: KalshiClient | None) -> tuple[pd.DataFrame, str]:
    if settings.prefer_real_outcome_labels and kalshi is not None:
        try:
            settled_markets = kalshi.fetch_settled_markets(limit=settings.settled_market_limit)
            real_frame = build_real_outcome_dataset(
                settled_markets=settled_markets,
                default_symbol=settings.training_symbol,
                lookback_days=max(730, settings.training_lookback_days),
            )
            if len(real_frame) >= settings.min_real_label_rows:
                out_path = Path(settings.real_labels_output_path)
                out_path.parent.mkdir(parents=True, exist_ok=True)
                real_frame.to_csv(out_path, index=False)
                return real_frame, "settled"

            print(
                "[WARN] settled labels unavailable or too small, falling back"
                f" rows={len(real_frame)} min_required={settings.min_real_label_rows}"
            )
        except Exception as exc:
            print(f"[WARN] failed building settled label dataset: {exc}")

    synthetic_frame = build_training_dataset(
        symbol=settings.training_symbol,
        lookback_days=settings.training_lookback_days,
    )
    synthetic_frame["label_source"] = "synthetic_proxy"
    return synthetic_frame, "synthetic"


def _market_quality_block(signal: MarketSignal, settings: Settings) -> str | None:
    if signal.volume is not None and signal.volume < settings.min_market_volume:
        return f"Low volume: {signal.volume}"

    spread = signal.spread_probability
    if spread is not None and spread > settings.max_spread_probability:
        return f"Wide spread: {spread:.3f}"

    return None


def _contract_template_block(signal: MarketSignal, settings: Settings) -> str | None:
    parsed = parse_event(signal.event_name, default_symbol=settings.training_symbol)
    if not parsed.is_price_contract and not settings.allow_generic_contracts:
        return "Unsupported contract template"
    return None


def _profile_market_payload(raw_markets: list[dict[str, object]]) -> dict[str, object]:
    total = len(raw_markets)
    with_ticker = 0
    with_title = 0
    with_yes_price = 0
    with_yes_bid = 0
    with_yes_ask = 0
    with_yes_bid_dollars = 0
    with_yes_ask_dollars = 0

    for item in raw_markets:
        if not isinstance(item, dict):
            continue
        if item.get("ticker") or item.get("market_ticker"):
            with_ticker += 1
        if item.get("title") or item.get("event_title"):
            with_title += 1
        if item.get("yes_price") or item.get("yesAsk") or item.get("yes_ask") or item.get("yes_ask_dollars"):
            with_yes_price += 1
        if item.get("yes_bid") or item.get("yesBid") or item.get("yes_bid_price"):
            with_yes_bid += 1
        if item.get("yes_ask") or item.get("yesAsk") or item.get("yes_price"):
            with_yes_ask += 1
        if item.get("yes_bid_dollars") is not None:
            with_yes_bid_dollars += 1
        if item.get("yes_ask_dollars") is not None:
            with_yes_ask_dollars += 1

    return {
        "raw_total": total,
        "with_ticker": with_ticker,
        "with_title": with_title,
        "with_yes_price_like": with_yes_price,
        "with_yes_bid_like": with_yes_bid,
        "with_yes_ask_like": with_yes_ask,
        "with_yes_bid_dollars": with_yes_bid_dollars,
        "with_yes_ask_dollars": with_yes_ask_dollars,
    }


def _live_symbol_hints(settings: Settings) -> list[str]:
    raw = settings.live_symbol_hints or ""
    return [token.strip().upper() for token in raw.split(",") if token.strip()]


def _live_topic_hints(settings: Settings) -> list[str]:
    raw = settings.live_topic_hints or ""
    return [token.strip() for token in raw.split(",") if token.strip()]


def _predict_generic_contract(signal: MarketSignal) -> tuple[float, float]:
    market = max(0.0, min(1.0, float(signal.market_probability)))
    volume = max(0.0, float(signal.volume or 0.0))
    open_interest = max(0.0, float(signal.open_interest or 0.0))

    liquidity = min(1.0, (volume + open_interest) / 2500.0)
    extremeness = abs(market - 0.5) * 2.0

    # Conservative mean-reversion prior for non-price markets.
    pull_strength = 0.06 + 0.14 * liquidity
    adjustment = (0.5 - market) * pull_strength
    model_prob = max(0.01, min(0.99, market + adjustment))

    confidence = max(0.50, min(0.78, 0.50 + 0.18 * liquidity + 0.10 * extremeness))
    return model_prob, confidence


def _reset_portfolio_state(settings: Settings) -> None:
    PORTFOLIO.bankroll_dollars = settings.bankroll_dollars
    PORTFOLIO.gross_exposure_dollars = 0.0
    PORTFOLIO.used_cash_dollars = 0.0
    PORTFOLIO.day_realized_pnl_dollars = 0.0
    PORTFOLIO.ticker_exposure_dollars.clear()
    PORTFOLIO.last_trade_unix_seconds.clear()


def _write_json_report(path: str, payload: dict[str, object]) -> None:
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


def _read_json_report(path: str) -> dict[str, object] | None:
    target = Path(path)
    if not target.exists():
        return None
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _effective_execution_gate(settings: Settings, decision: ExecutionGate | object) -> object:
    return decision


def _grade_gate_allows_live(settings: Settings) -> tuple[bool, str]:
    if not settings.enforce_grade_gate_for_live:
        return True, "grade gate disabled"

    report = _read_json_report(settings.monte_carlo_report_path)
    if not report:
        return False, "Live blocked: Monte Carlo grade report missing"

    grading = report.get("grading") if isinstance(report.get("grading"), dict) else {}
    status = str(grading.get("status", "unknown"))
    if status != "pass":
        return False, f"Live blocked: Monte Carlo grade status={status}"

    return True, "grade gate passed"


def _drift_gate_allows_live(settings: Settings) -> tuple[bool, str]:
    if not settings.enable_drift_live_downgrade:
        return True, "drift gate disabled"

    report = _read_json_report(settings.drift_report_path)
    if not report:
        return True, "drift report missing"

    drift = report.get("drift_detected")
    if drift is True:
        return False, "Live blocked: calibration drift detected"

    return True, "drift gate passed"


def _expected_calibration_error(y_true: pd.Series, probs: np.ndarray, bins: int = 10) -> float:
    if len(y_true) == 0:
        return 0.0

    edges = np.linspace(0.0, 1.0, bins + 1)
    y = y_true.to_numpy(dtype=float)
    ece = 0.0
    total = len(y)
    for idx in range(bins):
        lo = edges[idx]
        hi = edges[idx + 1]
        mask = (probs >= lo) & (probs < hi if idx < bins - 1 else probs <= hi)
        count = int(mask.sum())
        if count == 0:
            continue
        acc = float(y[mask].mean())
        conf = float(probs[mask].mean())
        ece += (count / total) * abs(acc - conf)
    return float(ece)


def _build_settled_walk_forward_report(settings: Settings, frame: pd.DataFrame) -> dict[str, object]:
    cols = ["moneyness", "days_to_expiry", "rv_20", "rv_60", "momentum_5d", "momentum_20d"]
    required = set(cols + ["label", "timestamp"])
    if frame.empty or not required.issubset(set(frame.columns)):
        return {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "rows": int(len(frame)),
            "status": "insufficient_data",
            "summary": {
                "fold_count": 0,
                "mean_brier": 0.0,
                "mean_log_loss": 0.0,
                "mean_ece": 0.0,
            },
        }
    data = frame.dropna(subset=cols + ["label", "timestamp"]).sort_values("timestamp").reset_index(drop=True)
    min_train = settings.settled_walk_forward_min_train_rows
    test_rows = settings.settled_walk_forward_test_rows
    if len(data) < min_train + test_rows:
        return {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "rows": int(len(data)),
            "status": "insufficient_data",
            "summary": {
                "fold_count": 0,
                "mean_brier": 0.0,
                "mean_log_loss": 0.0,
                "mean_ece": 0.0,
            },
        }

    vol_median = float(data["rv_20"].median())
    folds: list[dict[str, object]] = []

    start = min_train
    while start + test_rows <= len(data):
        train = data.iloc[:start]
        test = data.iloc[start : start + test_rows]

        model = ProbabilityModel()
        model.fit(train)
        probs = model.predict_batch(test[cols])
        y = test["label"].astype(int)

        fold: dict[str, object] = {
            "train_rows": int(len(train)),
            "test_rows": int(len(test)),
            "brier": float(brier_score_loss(y, probs)),
            "log_loss": float(log_loss(y, probs, labels=[0, 1])),
            "ece": _expected_calibration_error(y, probs),
            "regimes": {},
        }

        high = test["rv_20"] > vol_median
        low = ~high
        for name, mask in [("low_vol", low), ("high_vol", high)]:
            if int(mask.sum()) == 0:
                fold["regimes"][name] = {"rows": 0}
                continue
            y_r = y[mask]
            p_r = probs[mask.to_numpy()]
            fold["regimes"][name] = {
                "rows": int(mask.sum()),
                "brier": float(brier_score_loss(y_r, p_r)),
                "mean_label": float(y_r.mean()),
                "mean_pred": float(np.mean(p_r)),
            }

        folds.append(fold)
        start += test_rows

    mean_brier = float(np.mean([float(f["brier"]) for f in folds]))
    mean_log_loss = float(np.mean([float(f["log_loss"]) for f in folds]))
    mean_ece = float(np.mean([float(f["ece"]) for f in folds]))
    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "status": "ok",
        "rows": int(len(data)),
        "vol_regime_split_rv20_median": vol_median,
        "folds": folds,
        "summary": {
            "fold_count": len(folds),
            "mean_brier": mean_brier,
            "mean_log_loss": mean_log_loss,
            "mean_ece": mean_ece,
        },
    }


def _build_drift_report(settings: Settings, frame: pd.DataFrame) -> dict[str, object]:
    cols = ["moneyness", "days_to_expiry", "rv_20", "rv_60", "momentum_5d", "momentum_20d"]
    required = set(cols + ["label", "timestamp"])
    if frame.empty or not required.issubset(set(frame.columns)):
        return {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "drift_detected": False,
            "reason": "insufficient_rows",
            "recommended_mode": "insufficient_data",
            "rows": int(len(frame)),
        }
    data = frame.dropna(subset=cols + ["label", "timestamp"]).sort_values("timestamp").reset_index(drop=True)
    window = max(10, settings.drift_window_rows)
    if len(data) < (window * 2 + 30):
        return {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "drift_detected": False,
            "reason": "insufficient_rows",
            "recommended_mode": "insufficient_data",
            "rows": int(len(data)),
        }

    train = data.iloc[: -2 * window]
    baseline = data.iloc[-2 * window : -window]
    recent = data.iloc[-window:]
    if len(train) < 30:
        train = data.iloc[:window]

    model = ProbabilityModel()
    model.fit(train)

    y_base = baseline["label"].astype(int)
    p_base = model.predict_batch(baseline[cols])
    y_recent = recent["label"].astype(int)
    p_recent = model.predict_batch(recent[cols])

    base_brier = float(brier_score_loss(y_base, p_base))
    base_ece = _expected_calibration_error(y_base, p_base)
    recent_brier = float(brier_score_loss(y_recent, p_recent))
    recent_ece = _expected_calibration_error(y_recent, p_recent)

    brier_delta = recent_brier - base_brier
    ece_delta = recent_ece - base_ece
    drift_detected = (
        brier_delta >= settings.drift_brier_increase_threshold
        or ece_delta >= settings.drift_ece_increase_threshold
    )

    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "rows": int(len(data)),
        "window_rows": int(window),
        "baseline": {"brier": base_brier, "ece": base_ece},
        "recent": {"brier": recent_brier, "ece": recent_ece},
        "delta": {"brier": brier_delta, "ece": ece_delta},
        "thresholds": {
            "brier_increase": settings.drift_brier_increase_threshold,
            "ece_increase": settings.drift_ece_increase_threshold,
        },
        "drift_detected": drift_detected,
        "recommended_mode": "alert-only" if drift_detected else "live-ok",
    }


def _write_unrealized_pnl_report(
    settings: Settings,
    signals: list[MarketSignal],
    paper_ledger: PaperLedger,
) -> dict[str, object]:
    by_ticker = {item.ticker: item for item in signals}
    positions: list[dict[str, object]] = []
    total_unrealized = 0.0

    for pos in paper_ledger.positions:
        signal = by_ticker.get(pos.ticker)
        if signal is None:
            continue

        mark_prob = signal.market_probability if pos.side == "buy_yes" else (1.0 - signal.market_probability)
        unrealized = float((mark_prob - pos.entry_probability) * pos.quantity)
        total_unrealized += unrealized
        positions.append(
            {
                "ticker": pos.ticker,
                "side": pos.side,
                "quantity": pos.quantity,
                "entry_probability": pos.entry_probability,
                "mark_probability": mark_prob,
                "unrealized_pnl_dollars": unrealized,
            }
        )

    report = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "open_positions": len(positions),
        "total_unrealized_pnl_dollars": float(total_unrealized),
        "positions": positions,
    }
    _write_json_report(settings.unrealized_pnl_report_path, report)
    return report


def _recommendation_score(result: ScanResult) -> float:
    signal = result.signal
    confidence = float(signal.model_confidence or 0.0)
    spread_penalty = float(signal.spread_probability or 0.0)
    liquidity_bonus = min(1.0, float((signal.volume or 0)) / 200.0)
    return (
        abs(float(signal.net_edge)) * 100.0
        + confidence * 30.0
        + liquidity_bonus * 8.0
        - spread_penalty * 25.0
    )


def _write_recommendations_report(settings: Settings, results: list[ScanResult]) -> dict[str, object]:
    rows: list[dict[str, object]] = []
    for item in results:
        signal = item.signal
        if not item.should_alert:
            continue
        if item.order_quantity <= 0:
            continue
        confidence = float(signal.model_confidence or 0.0)
        if confidence < settings.recommendations_min_confidence:
            continue

        side = item.order_side or ("buy_yes" if signal.net_edge >= 0 else "buy_no")
        entry_price = signal.market_probability if side == "buy_yes" else (1.0 - signal.market_probability)
        est_cost = max(0.0, float(item.order_quantity) * float(entry_price))

        instruction = (
            f"Bet YES on {signal.event_name}" if side == "buy_yes" else f"Bet NO on {signal.event_name}"
        )
        rows.append(
            {
                "ticker": signal.ticker,
                "event_name": signal.event_name,
                "instruction": instruction,
                "side": side,
                "quantity": int(item.order_quantity),
                "estimated_cost_dollars": round(est_cost, 2),
                "estimated_value_dollars": round(float(item.expected_value_dollars), 2),
                "market_probability": round(float(signal.market_probability), 4),
                "model_probability": round(float(signal.model_probability), 4),
                "net_edge": round(float(signal.net_edge), 4),
                "confidence": round(confidence, 4),
                "spread_probability": round(float(signal.spread_probability or 0.0), 4),
                "volume": int(signal.volume or 0),
                "score": round(_recommendation_score(item), 4),
                "math": {
                    "edge_formula": "net_edge = (model_probability - market_probability) - costs",
                    "cost_formula": "estimated_cost = quantity * entry_price",
                    "ev_formula": "estimated_value = |net_edge| * quantity",
                },
            }
        )

    rows.sort(key=lambda row: float(row.get("score", 0.0)), reverse=True)
    items = rows[: max(1, settings.recommendations_max_items)]
    report = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "count": len(items),
        "items": items,
    }
    _write_json_report(settings.recommendations_report_path, report)
    return report


def _ensure_model(settings: Settings, kalshi: KalshiClient | None = None) -> None:
    if RUNTIME.model is None or not RUNTIME.model.is_fitted:
        _load_model_artifact(settings)

    should_retrain = (
        RUNTIME.model is None
        or not RUNTIME.model.is_fitted
        or (
            settings.retrain_every_n_scans > 0
            and RUNTIME.scan_count > 0
            and RUNTIME.scan_count % settings.retrain_every_n_scans == 0
        )
    )
    if not should_retrain:
        return

    previous_model = RUNTIME.model

    try:
        train, train_source = _choose_training_dataset(settings, kalshi)
        if len(train) < settings.min_training_rows:
            raise ValueError(
                f"Training data too small: rows={len(train)} min_required={settings.min_training_rows}"
            )

        model = ProbabilityModel()
        metrics = model.fit(train)
        wf = walk_forward_validate(
            train,
            min_train_rows=min(settings.min_training_rows, max(200, len(train) // 2)),
            test_rows=200,
        )

        RUNTIME.model = model
        RUNTIME.model_metrics = metrics
        RUNTIME.walk_forward = wf
        RUNTIME.training_rows = len(train)
        RUNTIME.training_data_source = train_source
        RUNTIME.model_source = "fresh-train"
        print(
            "[MODEL] trained"
            f" source={train_source}"
            f" rows={metrics.rows} brier={metrics.brier_score:.4f}"
            f" logloss={metrics.log_loss_score:.4f} ece={metrics.expected_calibration_error:.4f}"
            f" wf_folds={wf.folds} wf_brier={wf.mean_brier:.4f}"
        )
        _save_model_artifact(settings)
    except Exception as exc:
        if previous_model is not None and previous_model.is_fitted:
            RUNTIME.model = previous_model
            print(f"[WARN] model retrain failed, keeping previous model: {exc}")
            return
        raise


def _predict_from_model(signal: MarketSignal, settings: Settings) -> tuple[float, float]:
    parsed = parse_event(signal.event_name, default_symbol=settings.training_symbol)
    if parsed.strike is None or not parsed.is_price_contract:
        if settings.allow_generic_contracts:
            return _predict_generic_contract(signal)
        return signal.market_probability, 0.0

    if RUNTIME.model is None or not RUNTIME.model.is_fitted:
        return signal.market_probability, 0.0

    base_feats = make_live_features(
        symbol=parsed.symbol,
        strike=parsed.strike,
        days_to_expiry=parsed.days_to_expiry,
        lookback_days=max(260, min(700, settings.training_lookback_days)),
    )

    def _prob_above(strike: float) -> tuple[float, float]:
        row: dict[str, float | int] = {
            "moneyness": float(math.log(strike / base_feats.spot)),
            "days_to_expiry": base_feats.days_to_expiry,
            "rv_20": base_feats.rv_20,
            "rv_60": base_feats.rv_60,
            "momentum_5d": base_feats.momentum_5d,
            "momentum_20d": base_feats.momentum_20d,
        }
        return RUNTIME.model.predict_probability(row)

    p_above, c_above = _prob_above(parsed.strike)

    if parsed.direction == "above":
        return p_above, c_above

    if parsed.direction == "below":
        return max(0.0, min(1.0, 1.0 - p_above)), c_above

    if parsed.direction == "range" and parsed.strike_upper is not None:
        p_above_upper, c_upper = _prob_above(parsed.strike_upper)
        p_between = max(0.0, min(1.0, p_above - p_above_upper))
        return p_between, min(c_above, c_upper)

    return p_above, c_above


def build_and_export_real_labels(settings: Settings) -> pd.DataFrame:
    kalshi = KalshiClient(api_base=settings.kalshi_api_base, api_key=settings.kalshi_api_key)
    settled = kalshi.fetch_settled_markets(limit=settings.settled_market_limit)
    frame = build_real_outcome_dataset(
        settled_markets=settled,
        default_symbol=settings.training_symbol,
        lookback_days=max(730, settings.training_lookback_days),
    )
    if frame.empty:
        out_path = Path(settings.real_labels_output_path)
        if out_path.exists():
            out_path.unlink()
        return frame

    out_path = Path(settings.real_labels_output_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(out_path, index=False)
    return frame


def run_once(
    settings: Settings,
    log_file: Path,
    raw_markets_override: list[dict[str, object]] | None = None,
) -> list[ScanResult]:
    PORTFOLIO.bankroll_dollars = settings.bankroll_dollars
    kalshi = KalshiClient(api_base=settings.kalshi_api_base, api_key=settings.kalshi_api_key)
    executor = BrokerExecutor()
    paper_executor = PaperExecutor(
        log_path=settings.paper_trades_path,
        fill_probability=settings.paper_fill_probability,
        partial_fill_min_ratio=settings.paper_partial_fill_min_ratio,
        partial_fill_max_ratio=settings.paper_partial_fill_max_ratio,
        slippage_std_probability=settings.paper_slippage_std_probability,
        delay_ms_min=settings.paper_fill_delay_ms_min,
        delay_ms_max=settings.paper_fill_delay_ms_max,
    )
    paper_ledger = PaperLedger(
        state_path=settings.paper_ledger_state_path,
        event_log_path=settings.paper_trades_path,
    )
    risk = _risk_manager(settings)

    _ensure_model(settings, kalshi)
    RUNTIME.scan_count += 1

    execution_gate = ExecutionGate.check(
        enable_live_execution=settings.enable_live_execution,
        execution_mode=settings.execution_mode,
    )

    if execution_gate.execute:
        grade_ok, grade_reason = _grade_gate_allows_live(settings)
        if not grade_ok:
            execution_gate = type(execution_gate)(False, grade_reason)

    if execution_gate.execute and settings.enable_drift_live_downgrade and raw_markets_override is None:
        if settings.drift_check_every_n_scans > 0 and (RUNTIME.scan_count % settings.drift_check_every_n_scans == 0):
            try:
                settled = kalshi.fetch_settled_markets(limit=settings.settled_market_limit)
                frame = build_real_outcome_dataset(
                    settled_markets=settled,
                    default_symbol=settings.training_symbol,
                    lookback_days=max(730, settings.training_lookback_days),
                )
                drift_report = _build_drift_report(settings, frame)
                _write_json_report(settings.drift_report_path, drift_report)
            except Exception as exc:
                print(f"[WARN] drift monitor refresh failed: {exc}")

        drift_ok, drift_reason = _drift_gate_allows_live(settings)
        if not drift_ok:
            execution_gate = type(execution_gate)(False, drift_reason)

    print(f"[EXECUTION] {execution_gate.reason}")

    if (
        settings.reconcile_settlements_in_paper
        and not execution_gate.execute
        and raw_markets_override is None
    ):
        try:
            settled = kalshi.fetch_settled_markets(limit=settings.settled_market_limit)
            reconcile = paper_ledger.reconcile_settlements(settled, PORTFOLIO)
            if int(reconcile.get("closed_positions", 0)) > 0:
                print(
                    "[PAPER_LEDGER]"
                    f" closed={int(reconcile.get('closed_positions', 0))}"
                    f" realized_pnl={float(reconcile.get('realized_pnl_dollars', 0.0)):.2f}"
                )
        except Exception as exc:
            print(f"[WARN] settlement reconciliation failed: {exc}")

    fetch_meta: dict[str, object] = {"mode": "override" if raw_markets_override is not None else "live"}
    if raw_markets_override is not None:
        raw_markets = [item for item in raw_markets_override if isinstance(item, dict)]
    else:
        fetch_hints = [*_live_symbol_hints(settings), *_live_topic_hints(settings)]
        raw_markets, fetch_meta = kalshi.fetch_markets_with_fallback(
            limit=settings.live_market_limit,
            symbol_hint=settings.training_symbol,
            symbol_hints=fetch_hints,
        )
        if settings.record_market_snapshots:
            try:
                kalshi.save_markets_to_file(raw_markets, settings.market_snapshot_path)
            except Exception as exc:
                print(f"[WARN] failed to save market snapshot: {exc}")

    signals = kalshi.to_signal_candidates(raw_markets)
    try:
        _write_unrealized_pnl_report(settings=settings, signals=signals, paper_ledger=paper_ledger)
    except Exception as exc:
        print(f"[WARN] unrealized pnl report failed: {exc}")

    diagnostics = MarketDiagnostics(scanned=len(signals))
    diagnostics.metadata.update(
        {
            "fetch": fetch_meta,
            "payload_profile": _profile_market_payload(raw_markets),
            "signal_candidates": len(signals),
        }
    )

    results: list[ScanResult] = []
    alerted_count = 0

    for signal in signals:
        template_block = _contract_template_block(signal, settings)
        if template_block:
            diagnostics.add(
                reason=template_block,
                ticker=signal.ticker,
                event_name=signal.event_name,
                max_examples=settings.diagnostics_example_limit,
            )
            results.append(
                ScanResult(
                    signal=signal,
                    should_alert=False,
                    should_execute=False,
                    block_reason=template_block,
                )
            )
            continue

        quality_block = _market_quality_block(signal, settings)
        if quality_block:
            diagnostics.add(
                reason=quality_block,
                ticker=signal.ticker,
                event_name=signal.event_name,
                max_examples=settings.diagnostics_example_limit,
            )
            results.append(
                ScanResult(
                    signal=signal,
                    should_alert=False,
                    should_execute=False,
                    block_reason=quality_block,
                )
            )
            continue

        try:
            model_prob, confidence = _predict_from_model(signal, settings)
            cost_probability = _cost_probability(signal, settings)
            updated = MarketSignal(
                ticker=signal.ticker,
                event_name=signal.event_name,
                market_probability=signal.market_probability,
                model_probability=model_prob,
                calibrated_probability=model_prob,
                model_confidence=confidence,
                yes_bid=signal.yes_bid,
                yes_ask=signal.yes_ask,
                volume=signal.volume,
                open_interest=signal.open_interest,
                cost_probability=cost_probability,
            )

            is_opportunity = (
                abs(updated.net_edge) >= settings.min_edge_threshold
                and (updated.model_confidence or 0.0) >= settings.min_model_confidence
            )

            side = "buy_yes" if updated.net_edge > 0 else "buy_no"
            risk_decision = risk.approve(
                state=PORTFOLIO,
                ticker=updated.ticker,
                side=side,
                market_probability=updated.market_probability,
                model_probability=updated.calibrated_probability or updated.model_probability,
                net_edge=updated.net_edge,
                confidence=updated.model_confidence or 0.0,
            )

            should_trade = is_opportunity and risk_decision.allowed
            should_execute = should_trade and execution_gate.execute

            block_reason = None
            if not risk_decision.allowed:
                block_reason = risk_decision.reason
            elif should_trade and not execution_gate.execute:
                block_reason = execution_gate.reason

            if block_reason:
                diagnostics.add(
                    reason=block_reason,
                    ticker=updated.ticker,
                    event_name=updated.event_name,
                    max_examples=settings.diagnostics_example_limit,
                )

            result = ScanResult(
                signal=updated,
                should_alert=is_opportunity,
                should_execute=should_execute,
                order_side=side,
                order_quantity=risk_decision.quantity,
                expected_value_dollars=risk_decision.expected_value_dollars,
                block_reason=block_reason,
            )
            results.append(result)

            if result.should_alert:
                if alerted_count < settings.max_alerts_per_scan:
                    alerted_count += 1
                    message = (
                        f"Opportunity {updated.ticker}: net_edge={updated.net_edge:.3f}, raw_edge={updated.edge:.3f}, "
                        f"market={updated.market_probability:.3f}, model={updated.model_probability:.3f}, "
                        f"confidence={(updated.model_confidence or 0.0):.3f}, side={side}, "
                        f"qty={result.order_quantity}, ev=${result.expected_value_dollars:.2f}, "
                        f"spread={updated.spread_probability}, volume={updated.volume}, "
                        f"block_reason={result.block_reason}"
                    )
                    send_alert(settings.alert_webhook_url, message)
                    append_log(log_file, updated)

                if result.should_execute:
                    executor.place_order(updated.ticker, side=side, quantity=result.order_quantity)
                    risk.register_fill(
                        state=PORTFOLIO,
                        ticker=updated.ticker,
                        side=side,
                        quantity=result.order_quantity,
                        market_probability=updated.market_probability,
                    )
                elif should_trade and not execution_gate.execute:
                    fill = paper_executor.place_order(
                        market_ticker=updated.ticker,
                        side=side,
                        quantity=result.order_quantity,
                        market_probability=updated.market_probability,
                        model_probability=updated.model_probability,
                        net_edge=updated.net_edge,
                        confidence=updated.model_confidence or 0.0,
                    )
                    filled_quantity = int(fill.get("filled_quantity", 0))
                    effective_probability = float(fill.get("effective_probability", updated.market_probability))
                    if filled_quantity <= 0:
                        continue
                    paper_ledger.add_position(
                        ticker=updated.ticker,
                        side=side,
                        quantity=filled_quantity,
                        market_probability=effective_probability,
                    )
                    risk.register_fill(
                        state=PORTFOLIO,
                        ticker=updated.ticker,
                        side=side,
                        quantity=filled_quantity,
                        market_probability=effective_probability,
                    )
        except Exception as exc:
            diagnostics.add(
                reason=f"Score failure: {type(exc).__name__}",
                ticker=signal.ticker,
                event_name=signal.event_name,
                max_examples=settings.diagnostics_example_limit,
            )
            print(f"[WARN] failed to score market {signal.ticker}: {exc}")

    try:
        diagnostics.save_json(settings.diagnostics_report_path)
    except Exception as exc:
        print(f"[WARN] failed writing diagnostics report: {exc}")

    if diagnostics.by_reason:
        summary = ", ".join(f"{k}={v}" for k, v in sorted(diagnostics.by_reason.items()))
        print(f"[DIAGNOSTICS] {summary}")

    try:
        _write_recommendations_report(settings=settings, results=results)
    except Exception as exc:
        print(f"[WARN] recommendations report failed: {exc}")

    return results


def run_once_from_file(settings: Settings, log_file: Path, input_file: str) -> list[ScanResult]:
    markets = KalshiClient.load_markets_from_file(input_file)
    return run_once(settings=settings, log_file=log_file, raw_markets_override=markets)


def diagnose_markets(
    settings: Settings,
    input_file: str | None = None,
) -> MarketDiagnostics:
    kalshi = KalshiClient(api_base=settings.kalshi_api_base, api_key=settings.kalshi_api_key)
    fetch_meta: dict[str, object] = {"mode": "file" if input_file else "live"}
    if input_file:
        raw = KalshiClient.load_markets_from_file(input_file)
    else:
        raw, fetch_meta = kalshi.fetch_markets_with_fallback(
            limit=settings.live_market_limit,
            symbol_hint=settings.training_symbol,
            symbol_hints=_live_symbol_hints(settings),
        )

    signals = kalshi.to_signal_candidates(raw)
    diagnostics = MarketDiagnostics(scanned=len(signals))
    diagnostics.metadata.update(
        {
            "fetch": fetch_meta,
            "payload_profile": _profile_market_payload(raw),
            "signal_candidates": len(signals),
        }
    )
    for signal in signals:
        template_block = _contract_template_block(signal, settings)
        if template_block:
            diagnostics.add(template_block, signal.ticker, signal.event_name, settings.diagnostics_example_limit)
            continue

        quality_block = _market_quality_block(signal, settings)
        if quality_block:
            diagnostics.add(quality_block, signal.ticker, signal.event_name, settings.diagnostics_example_limit)
            continue

        diagnostics.add("Passes pre-score filters", signal.ticker, signal.event_name, settings.diagnostics_example_limit)

    diagnostics.save_json(settings.diagnostics_report_path)
    return diagnostics


def diagnose_fetch_sources(settings: Settings, input_file: str | None = None) -> dict[str, object]:
    kalshi = KalshiClient(api_base=settings.kalshi_api_base, api_key=settings.kalshi_api_key)

    if input_file:
        rows = KalshiClient.load_markets_from_file(input_file)
        signals = kalshi.to_signal_candidates(rows)
        template_block = 0
        quality_block = 0
        pre_score_pass = 0
        for signal in signals:
            if _contract_template_block(signal, settings):
                template_block += 1
                continue
            if _market_quality_block(signal, settings):
                quality_block += 1
                continue
            pre_score_pass += 1

        report = {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "training_symbol": settings.training_symbol,
            "requested_hints": _live_symbol_hints(settings),
            "attempt_count": 1,
            "best_attempt_index": 0,
            "best_pre_score_pass": pre_score_pass,
            "sources": [
                {
                    "params": {"mode": "file", "input_file": input_file},
                    "raw_count": len(rows),
                    "signal_candidates": len(signals),
                    "template_block": template_block,
                    "quality_block": quality_block,
                    "pre_score_pass": pre_score_pass,
                    "error": None,
                }
            ],
        }
        out = Path(settings.source_cascade_report_path)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(report, ensure_ascii=True, indent=2), encoding="utf-8")
        return report

    attempts = kalshi.fetch_markets_source_cascade(
        limit=settings.live_market_limit,
        symbol_hint=settings.training_symbol,
        symbol_hints=_live_symbol_hints(settings),
        max_attempts=settings.source_diagnostics_max_attempts,
    )

    sources: list[dict[str, object]] = []
    best_index = -1
    best_passes = -1

    for idx, attempt in enumerate(attempts):
        params = attempt.get("params") if isinstance(attempt.get("params"), dict) else {}
        rows = attempt.get("rows") if isinstance(attempt.get("rows"), list) else []
        error = attempt.get("error")

        signals = kalshi.to_signal_candidates(rows)
        template_block = 0
        quality_block = 0
        pre_score_pass = 0

        for signal in signals:
            if _contract_template_block(signal, settings):
                template_block += 1
                continue
            if _market_quality_block(signal, settings):
                quality_block += 1
                continue
            pre_score_pass += 1

        item = {
            "params": params,
            "raw_count": int(attempt.get("count", 0)),
            "signal_candidates": len(signals),
            "template_block": template_block,
            "quality_block": quality_block,
            "pre_score_pass": pre_score_pass,
            "error": error,
        }
        sources.append(item)

        if error is None and pre_score_pass > best_passes:
            best_passes = pre_score_pass
            best_index = idx

    report = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "training_symbol": settings.training_symbol,
        "requested_hints": _live_symbol_hints(settings),
        "attempt_count": len(sources),
        "best_attempt_index": best_index,
        "best_pre_score_pass": best_passes,
        "sources": sources,
    }

    out = Path(settings.source_cascade_report_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, ensure_ascii=True, indent=2), encoding="utf-8")
    return report


def run_paper_cycle_from_files(
    settings: Settings,
    log_file: Path,
    market_file: str,
    settled_file: str,
    reset_state: bool = True,
) -> dict[str, object]:
    if reset_state:
        _reset_portfolio_state(settings)
        for file_path in [settings.paper_trades_path, settings.paper_ledger_state_path]:
            target = Path(file_path)
            if target.exists():
                target.unlink()

    results = run_once_from_file(settings=settings, log_file=log_file, input_file=market_file)

    opened_paper_positions = sum(
        1
        for item in results
        if item.order_quantity > 0 and item.block_reason == "Paper mode active."
    )

    settled_rows = KalshiClient.load_markets_from_file(settled_file)
    ledger = PaperLedger(
        state_path=settings.paper_ledger_state_path,
        event_log_path=settings.paper_trades_path,
    )
    reconcile = ledger.reconcile_settlements(settled_rows, PORTFOLIO)

    summary = {
        "markets_scanned": len(results),
        "alerts": sum(1 for item in results if item.should_alert),
        "opened_paper_positions": opened_paper_positions,
        "reconciled_closed_positions": int(reconcile.get("closed_positions", 0)),
        "reconciled_realized_pnl_dollars": float(reconcile.get("realized_pnl_dollars", 0.0)),
        "portfolio_bankroll_dollars": PORTFOLIO.bankroll_dollars,
        "portfolio_used_cash_dollars": PORTFOLIO.used_cash_dollars,
        "market_file": market_file,
        "settled_file": settled_file,
    }

    report = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "summary": summary,
    }
    out = Path(settings.paper_cycle_report_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, ensure_ascii=True, indent=2), encoding="utf-8")
    return summary


def _parse_monte_carlo_sizes(raw: str, max_trades: int) -> list[int]:
    values: list[int] = []
    for token in raw.split(","):
        token = token.strip()
        if not token:
            continue
        try:
            n = int(token)
        except ValueError:
            continue
        if n > 0:
            values.append(n)
    if not values:
        values = [10, 25, 50, 100, 250, 500, 1000]

    deduped = sorted(set(values))
    return [n for n in deduped if n <= max(1, max_trades)]


def _read_settlement_pnl_series(path: str) -> list[float]:
    values: list[float] = []
    target = Path(path)
    if not target.exists():
        return values

    for line in target.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if row.get("event_type") != "paper_settlement":
            continue
        pnl = row.get("realized_pnl_dollars")
        if isinstance(pnl, (int, float)):
            values.append(float(pnl))
    return values


def _build_monte_carlo_report(
    pnl_series: list[float],
    seed: int,
    trials: int,
    sample_sizes: list[int],
    min_settled_trades_for_grade: int,
    health_pass_score: float,
    min_positive_prob_at_max_n: float,
    context: dict[str, object],
) -> dict[str, object]:
    pnl_array = np.array(pnl_series, dtype=float)
    empirical_mean = float(np.mean(pnl_array))
    empirical_std = float(np.std(pnl_array))
    empirical_win_rate = float(np.mean(pnl_array > 0.0))

    rng = np.random.default_rng(seed)
    convergence: list[dict[str, float]] = []

    for sample_size in sample_sizes:
        draws = rng.choice(pnl_array, size=(trials, sample_size), replace=True)
        means = draws.mean(axis=1)
        totals = draws.sum(axis=1)
        convergence.append(
            {
                "sample_size": float(sample_size),
                "mean_of_means": float(np.mean(means)),
                "std_of_means": float(np.std(means)),
                "abs_error_vs_empirical_mean": float(abs(np.mean(means) - empirical_mean)),
                "prob_total_pnl_positive": float(np.mean(totals > 0.0)),
                "p05_total_pnl": float(np.percentile(totals, 5)),
                "p50_total_pnl": float(np.percentile(totals, 50)),
                "p95_total_pnl": float(np.percentile(totals, 95)),
            }
        )

    max_n = max(sample_sizes)
    max_row = next(
        (item for item in convergence if int(item["sample_size"]) == max_n),
        convergence[-1],
    )

    boot_draws = rng.choice(pnl_array, size=(trials, len(pnl_array)), replace=True)
    boot_means = boot_draws.mean(axis=1)
    ci95 = {
        "lower": float(np.percentile(boot_means, 2.5)),
        "upper": float(np.percentile(boot_means, 97.5)),
    }

    warning = None
    if len(pnl_series) < 30:
        warning = "Low settled trade count (<30): Monte Carlo uncertainty is high."

    grade_eligible = len(pnl_series) >= max(1, min_settled_trades_for_grade)
    checks = {
        "mean_positive": empirical_mean > 0.0,
        "ci95_lower_positive": ci95["lower"] > 0.0,
        "win_rate_at_least_half": empirical_win_rate >= 0.5,
        "positive_prob_at_max_n": float(max_row["prob_total_pnl_positive"]) >= min_positive_prob_at_max_n,
    }
    health_score = float(sum(1 for passed in checks.values() if passed) / len(checks))

    if not grade_eligible:
        health_status = "insufficient_data"
    elif health_score >= health_pass_score:
        health_status = "pass"
    else:
        health_status = "fail"

    grading = {
        "eligible": grade_eligible,
        "required_min_settled_trades": int(min_settled_trades_for_grade),
        "health_pass_score_threshold": float(health_pass_score),
        "min_positive_prob_at_max_n": float(min_positive_prob_at_max_n),
        "max_sample_size_evaluated": int(max_n),
        "checks": checks,
        "health_score": health_score,
        "status": health_status,
    }

    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "seed": seed,
        "trials": trials,
        "empirical_trade_count": len(pnl_series),
        "empirical_mean_trade_pnl": empirical_mean,
        "empirical_std_trade_pnl": empirical_std,
        "empirical_win_rate": empirical_win_rate,
        "bootstrap_ci95_mean_trade_pnl": ci95,
        "warning": warning,
        "grading": grading,
        "convergence": convergence,
        **context,
    }


def run_monte_carlo_validation_from_files(
    settings: Settings,
    log_file: Path,
    market_file: str,
    settled_file: str,
) -> dict[str, object]:
    cycle_summary = run_paper_cycle_from_files(
        settings=settings,
        log_file=log_file,
        market_file=market_file,
        settled_file=settled_file,
        reset_state=True,
    )
    pnl_series = _read_settlement_pnl_series(settings.paper_trades_path)
    if not pnl_series:
        raise ValueError("Monte Carlo requires at least one settled paper trade.")

    sample_sizes = _parse_monte_carlo_sizes(settings.monte_carlo_sample_sizes, settings.monte_carlo_max_trades)
    if not sample_sizes:
        raise ValueError("No valid Monte Carlo sample sizes found.")

    trials = max(100, settings.monte_carlo_trials)
    summary = _build_monte_carlo_report(
        pnl_series=pnl_series,
        seed=settings.monte_carlo_seed,
        trials=trials,
        sample_sizes=sample_sizes,
        min_settled_trades_for_grade=settings.monte_carlo_min_settled_trades_for_grade,
        health_pass_score=settings.monte_carlo_health_pass_score,
        min_positive_prob_at_max_n=settings.monte_carlo_min_positive_prob_at_max_n,
        context={
            "mode": "files",
        "market_file": market_file,
        "settled_file": settled_file,
        "paper_cycle_summary": cycle_summary,
        },
    )

    out = Path(settings.monte_carlo_report_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(summary, ensure_ascii=True, indent=2), encoding="utf-8")
    return summary


def run_monte_carlo_validation_from_journal(settings: Settings) -> dict[str, object]:
    pnl_series = _read_settlement_pnl_series(settings.paper_trades_path)
    if not pnl_series:
        raise ValueError("No settled paper trades found in journal for Monte Carlo.")

    sample_sizes = _parse_monte_carlo_sizes(settings.monte_carlo_sample_sizes, settings.monte_carlo_max_trades)
    if not sample_sizes:
        raise ValueError("No valid Monte Carlo sample sizes found.")

    trials = max(100, settings.monte_carlo_trials)
    summary = _build_monte_carlo_report(
        pnl_series=pnl_series,
        seed=settings.monte_carlo_seed,
        trials=trials,
        sample_sizes=sample_sizes,
        min_settled_trades_for_grade=settings.monte_carlo_min_settled_trades_for_grade,
        health_pass_score=settings.monte_carlo_health_pass_score,
        min_positive_prob_at_max_n=settings.monte_carlo_min_positive_prob_at_max_n,
        context={
            "mode": "journal",
            "paper_trades_path": settings.paper_trades_path,
        },
    )

    out = Path(settings.monte_carlo_report_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(summary, ensure_ascii=True, indent=2), encoding="utf-8")
    return summary


def run_paper_cycle_suite(
    settings: Settings,
    log_file: Path,
    market_file: str,
    settled_file: str,
) -> dict[str, object]:
    source_report = diagnose_fetch_sources(settings=settings, input_file=market_file)
    paper_cycle_summary = run_paper_cycle_from_files(
        settings=settings,
        log_file=log_file,
        market_file=market_file,
        settled_file=settled_file,
        reset_state=True,
    )
    return {
        "source_report_path": settings.source_cascade_report_path,
        "paper_cycle_report_path": settings.paper_cycle_report_path,
        "source_best_pre_score_pass": int(source_report.get("best_pre_score_pass", 0)),
        "markets_scanned": int(paper_cycle_summary.get("markets_scanned", 0)),
        "opened_paper_positions": int(paper_cycle_summary.get("opened_paper_positions", 0)),
        "reconciled_closed_positions": int(paper_cycle_summary.get("reconciled_closed_positions", 0)),
        "reconciled_realized_pnl_dollars": float(
            paper_cycle_summary.get("reconciled_realized_pnl_dollars", 0.0)
        ),
    }


def run_settled_walk_forward_validation(
    settings: Settings,
    settled_file: str | None = None,
) -> dict[str, object]:
    if settled_file:
        settled = KalshiClient.load_markets_from_file(settled_file)
    else:
        kalshi = KalshiClient(api_base=settings.kalshi_api_base, api_key=settings.kalshi_api_key)
        settled = kalshi.fetch_settled_markets(limit=settings.settled_market_limit)

    frame = build_real_outcome_dataset(
        settled_markets=settled,
        default_symbol=settings.training_symbol,
        lookback_days=max(730, settings.training_lookback_days),
    )
    report = _build_settled_walk_forward_report(settings, frame)
    report["source"] = "file" if settled_file else "live"
    if settled_file:
        report["settled_file"] = settled_file
    _write_json_report(settings.settled_walk_forward_report_path, report)
    return report


def run_calibration_drift_monitor(
    settings: Settings,
    settled_file: str | None = None,
) -> dict[str, object]:
    if settled_file:
        settled = KalshiClient.load_markets_from_file(settled_file)
    else:
        kalshi = KalshiClient(api_base=settings.kalshi_api_base, api_key=settings.kalshi_api_key)
        settled = kalshi.fetch_settled_markets(limit=settings.settled_market_limit)

    frame = build_real_outcome_dataset(
        settled_markets=settled,
        default_symbol=settings.training_symbol,
        lookback_days=max(730, settings.training_lookback_days),
    )
    report = _build_drift_report(settings, frame)
    report["source"] = "file" if settled_file else "live"
    if settled_file:
        report["settled_file"] = settled_file
    _write_json_report(settings.drift_report_path, report)
    return report


def run_health_dashboard(
    settings: Settings,
    refresh_drift: bool = False,
    refresh_monte_carlo_journal: bool = False,
    settled_file: str | None = None,
) -> dict[str, object]:
    errors: list[str] = []
    if refresh_drift:
        try:
            run_calibration_drift_monitor(settings=settings, settled_file=settled_file)
        except Exception as exc:
            errors.append(f"drift_refresh_failed: {exc}")

    if refresh_monte_carlo_journal:
        try:
            run_monte_carlo_validation_from_journal(settings=settings)
        except Exception as exc:
            errors.append(f"monte_carlo_refresh_failed: {exc}")

    monte_carlo = _read_json_report(settings.monte_carlo_report_path) or {}
    drift = _read_json_report(settings.drift_report_path) or {}
    settled_wf = _read_json_report(settings.settled_walk_forward_report_path) or {}
    unrealized = _read_json_report(settings.unrealized_pnl_report_path) or {}

    grading = monte_carlo.get("grading") if isinstance(monte_carlo.get("grading"), dict) else {}
    grade_status = str(grading.get("status", "missing"))
    drift_detected = bool(drift.get("drift_detected")) if drift else False
    wf_status = str(settled_wf.get("status", "missing"))

    checks = {
        "grade_status_pass": grade_status == "pass",
        "drift_not_detected": not drift_detected,
        "settled_walk_forward_available": wf_status == "ok",
    }

    failed = [name for name, ok in checks.items() if not ok]
    decision = "go" if not failed else "no-go"
    reasons: list[str] = []
    if failed:
        if "grade_status_pass" in failed:
            reasons.append(f"grade_status={grade_status}")
        if "drift_not_detected" in failed:
            reasons.append("calibration_drift_detected")
        if "settled_walk_forward_available" in failed:
            reasons.append(f"settled_walk_forward_status={wf_status}")

    summary = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "decision": decision,
        "checks": checks,
        "failure_reasons": reasons,
        "refresh_errors": errors,
        "inputs": {
            "monte_carlo_report_path": settings.monte_carlo_report_path,
            "drift_report_path": settings.drift_report_path,
            "settled_walk_forward_report_path": settings.settled_walk_forward_report_path,
            "unrealized_pnl_report_path": settings.unrealized_pnl_report_path,
        },
        "snapshot": {
            "monte_carlo": {
                "empirical_trade_count": monte_carlo.get("empirical_trade_count"),
                "empirical_mean_trade_pnl": monte_carlo.get("empirical_mean_trade_pnl"),
                "grade_status": grade_status,
                "grade_score": grading.get("health_score"),
            },
            "drift": {
                "drift_detected": drift.get("drift_detected"),
                "recommended_mode": drift.get("recommended_mode"),
            },
            "settled_walk_forward": {
                "status": wf_status,
                "summary": settled_wf.get("summary") if isinstance(settled_wf.get("summary"), dict) else None,
            },
            "unrealized": {
                "open_positions": unrealized.get("open_positions"),
                "total_unrealized_pnl_dollars": unrealized.get("total_unrealized_pnl_dollars"),
            },
        },
    }

    _write_json_report(settings.health_dashboard_report_path, summary)
    return summary


def update_settled_archive(
    settings: Settings,
    settled_file: str | None = None,
) -> dict[str, object]:
    if settled_file:
        settled = KalshiClient.load_markets_from_file(settled_file)
    else:
        kalshi = KalshiClient(api_base=settings.kalshi_api_base, api_key=settings.kalshi_api_key)
        settled = kalshi.fetch_settled_markets(limit=settings.settled_market_limit)

    normalized = normalize_settled_markets(settled)
    archive_path = Path(settings.settled_archive_path)
    existing: list[dict[str, object]] = []
    if archive_path.exists():
        try:
            payload = json.loads(archive_path.read_text(encoding="utf-8"))
            if isinstance(payload, list):
                existing = [row for row in payload if isinstance(row, dict)]
        except Exception:
            existing = []

    keyset: set[str] = set()
    for row in existing:
        key = f"{row.get('ticker')}|{row.get('settled_at')}"
        keyset.add(key)

    added = 0
    for item in normalized:
        row = {
            "ticker": item.ticker,
            "event_name": item.event_name,
            "settled_at": item.settled_at.isoformat(),
            "yes_outcome": int(item.yes_outcome),
        }
        key = f"{row['ticker']}|{row['settled_at']}"
        if key in keyset:
            continue
        keyset.add(key)
        existing.append(row)
        added += 1

    existing.sort(key=lambda r: str(r.get("settled_at", "")), reverse=True)
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    archive_path.write_text(json.dumps(existing, ensure_ascii=True, indent=2), encoding="utf-8")

    return {
        "archive_path": settings.settled_archive_path,
        "added_rows": added,
        "total_rows": len(existing),
        "source": "file" if settled_file else "live",
    }


def run_evidence_cycle(
    settings: Settings,
    settled_file: str | None = None,
) -> dict[str, object]:
    archive = update_settled_archive(settings=settings, settled_file=settled_file)

    archive_rows = KalshiClient.load_markets_from_file(settings.settled_archive_path)
    settled_rows: list[dict[str, object]] = []
    for row in archive_rows:
        if not isinstance(row, dict):
            continue
        if "yes_outcome" in row and "settled_at" in row:
            settled_rows.append(
                {
                    "ticker": row.get("ticker"),
                    "title": row.get("event_name") or row.get("title") or row.get("ticker"),
                    "yes_settle": row.get("yes_outcome"),
                    "settled_time": row.get("settled_at"),
                }
            )
        else:
            settled_rows.append(row)

    frame = build_real_outcome_dataset(
        settled_markets=settled_rows,
        default_symbol=settings.training_symbol,
        lookback_days=max(730, settings.training_lookback_days),
    )

    drift_report = _build_drift_report(settings, frame)
    drift_report["source"] = "archive"
    _write_json_report(settings.drift_report_path, drift_report)

    wf_report = _build_settled_walk_forward_report(settings, frame)
    wf_report["source"] = "archive"
    _write_json_report(settings.settled_walk_forward_report_path, wf_report)

    monte_error = None
    try:
        run_monte_carlo_validation_from_journal(settings=settings)
    except Exception as exc:
        monte_error = str(exc)

    dashboard = run_health_dashboard(settings=settings)
    monte = _read_json_report(settings.monte_carlo_report_path) or {}
    grade = monte.get("grading") if isinstance(monte.get("grading"), dict) else {}
    empirical_trade_count = int(monte.get("empirical_trade_count", 0) or 0)
    min_trades = int(settings.monte_carlo_min_settled_trades_for_grade)

    needed_trades = max(0, min_trades - empirical_trade_count)
    needed_wf_rows = max(
        0,
        int(settings.settled_walk_forward_min_train_rows + settings.settled_walk_forward_test_rows)
        - int(wf_report.get("rows", 0) or 0),
    )

    report = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "archive": archive,
        "drift": {
            "drift_detected": drift_report.get("drift_detected"),
            "recommended_mode": drift_report.get("recommended_mode"),
        },
        "settled_walk_forward": {
            "status": wf_report.get("status"),
            "rows": wf_report.get("rows"),
            "summary": wf_report.get("summary"),
        },
        "monte_carlo": {
            "error": monte_error,
            "empirical_trade_count": empirical_trade_count,
            "grade_status": grade.get("status", "missing"),
            "grade_score": grade.get("health_score"),
        },
        "health_dashboard": {
            "decision": dashboard.get("decision"),
            "failure_reasons": dashboard.get("failure_reasons"),
        },
        "next_targets": {
            "additional_settled_trades_for_grade": needed_trades,
            "additional_settled_rows_for_walk_forward": needed_wf_rows,
        },
    }
    _write_json_report(settings.evidence_cycle_report_path, report)
    return report


def run_loop(settings: Settings, log_file: Path) -> None:
    print("Starting market discrepancy scanner...")
    print(f"scan_interval_seconds={settings.scan_interval_seconds}")
    print(f"min_edge_threshold={settings.min_edge_threshold}")
    print(f"execution_mode={settings.execution_mode}")
    print(f"training_symbol={settings.training_symbol}")
    print(f"min_model_confidence={settings.min_model_confidence}")

    while True:
        try:
            results = run_once(settings, log_file)
            print(f"Scan complete. markets_scanned={len(results)}")
        except Exception as exc:
            print(f"[ERROR] scan failed: {exc}")

        time.sleep(settings.scan_interval_seconds)
