from __future__ import annotations

import os
from dataclasses import dataclass


def _as_bool(value: str, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Settings:
    scan_interval_seconds: int = 30
    min_edge_threshold: float = 0.05
    alert_webhook_url: str | None = None

    # Model/training controls.
    training_symbol: str = "SPY"
    training_lookback_days: int = 1500
    min_model_confidence: float = 0.55
    min_training_rows: int = 400
    retrain_every_n_scans: int = 50
    model_artifact_path: str = "data/model/latest.joblib"
    model_max_age_minutes: int = 360
    prefer_real_outcome_labels: bool = True
    settled_market_limit: int = 600
    min_real_label_rows: int = 120
    real_labels_output_path: str = "data/labels/settled_labels.csv"
    settled_archive_path: str = "data/labels/settled_markets_archive.json"

    # Safety gates for trading execution.
    enable_live_execution: bool = False
    execution_mode: str = "paper"  # paper | live

    # Transaction cost model (in probability points, not dollars).
    fee_rate_probability: float = 0.01
    slippage_probability: float = 0.005

    # Portfolio/risk controls.
    bankroll_dollars: float = 5000.0
    max_exposure_dollars: float = 800.0
    max_ticker_exposure_dollars: float = 250.0
    max_open_tickers: int = 6
    max_trade_size_dollars: float = 100.0
    daily_loss_limit_dollars: float = 150.0
    kelly_fraction: float = 0.20
    per_ticker_cooldown_seconds: int = 300

    # Market quality controls.
    min_market_volume: int = 25
    max_spread_probability: float = 0.08
    min_expected_value_dollars_per_contract: float = 0.015
    max_alerts_per_scan: int = 10

    # Paper journaling.
    paper_trades_path: str = "data/paper_trades.jsonl"
    paper_ledger_state_path: str = "data/paper_ledger_state.json"
    reconcile_settlements_in_paper: bool = True
    paper_fill_probability: float = 0.95
    paper_partial_fill_min_ratio: float = 0.60
    paper_partial_fill_max_ratio: float = 1.00
    paper_slippage_std_probability: float = 0.01
    paper_fill_delay_ms_min: int = 20
    paper_fill_delay_ms_max: int = 250

    # Data capture and replay.
    record_market_snapshots: bool = True
    market_snapshot_path: str = "data/raw/latest_markets.json"
    replay_input_path: str = "data/samples/sample_markets.json"
    replay_settled_input_path: str = "data/samples/sample_settled_markets.json"
    live_market_limit: int = 200
    live_symbol_hints: str = "SPY,QQQ,IWM,DIA,NDX,SPX"

    # Diagnostics.
    diagnostics_report_path: str = "data/diagnostics/latest_report.json"
    source_cascade_report_path: str = "data/diagnostics/source_cascade_report.json"
    paper_cycle_report_path: str = "data/diagnostics/paper_cycle_report.json"
    monte_carlo_report_path: str = "data/diagnostics/monte_carlo_report.json"
    unrealized_pnl_report_path: str = "data/diagnostics/unrealized_pnl_report.json"
    settled_walk_forward_report_path: str = "data/diagnostics/settled_walk_forward_report.json"
    drift_report_path: str = "data/diagnostics/drift_report.json"
    health_dashboard_report_path: str = "data/diagnostics/health_dashboard_report.json"
    evidence_cycle_report_path: str = "data/diagnostics/evidence_cycle_report.json"
    recommendations_report_path: str = "data/diagnostics/recommendations_report.json"
    diagnostics_example_limit: int = 5
    source_diagnostics_max_attempts: int = 30
    monte_carlo_trials: int = 2000
    monte_carlo_max_trades: int = 1000
    monte_carlo_sample_sizes: str = "10,25,50,100,250,500,1000"
    monte_carlo_seed: int = 42
    monte_carlo_min_settled_trades_for_grade: int = 50
    monte_carlo_health_pass_score: float = 0.75
    monte_carlo_min_positive_prob_at_max_n: float = 0.60
    enforce_grade_gate_for_live: bool = True
    settled_walk_forward_min_train_rows: int = 200
    settled_walk_forward_test_rows: int = 50
    drift_window_rows: int = 100
    drift_brier_increase_threshold: float = 0.03
    drift_ece_increase_threshold: float = 0.02
    drift_check_every_n_scans: int = 20
    enable_drift_live_downgrade: bool = True
    recommendations_max_items: int = 12
    recommendations_min_confidence: float = 0.55

    kalshi_api_base: str = "https://api.elections.kalshi.com"
    kalshi_api_key: str | None = None
    kalshi_private_key_path: str | None = None


def load_settings() -> Settings:
    return Settings(
        scan_interval_seconds=int(os.getenv("SCAN_INTERVAL_SECONDS", "30")),
        min_edge_threshold=float(os.getenv("MIN_EDGE_THRESHOLD", "0.05")),
        alert_webhook_url=os.getenv("ALERT_WEBHOOK_URL"),
        training_symbol=os.getenv("TRAINING_SYMBOL", "SPY"),
        training_lookback_days=int(os.getenv("TRAINING_LOOKBACK_DAYS", "1500")),
        min_model_confidence=float(os.getenv("MIN_MODEL_CONFIDENCE", "0.55")),
        min_training_rows=int(os.getenv("MIN_TRAINING_ROWS", "400")),
        retrain_every_n_scans=int(os.getenv("RETRAIN_EVERY_N_SCANS", "50")),
        model_artifact_path=os.getenv("MODEL_ARTIFACT_PATH", "data/model/latest.joblib"),
        model_max_age_minutes=int(os.getenv("MODEL_MAX_AGE_MINUTES", "360")),
        prefer_real_outcome_labels=_as_bool(os.getenv("PREFER_REAL_OUTCOME_LABELS", "true")),
        settled_market_limit=int(os.getenv("SETTLED_MARKET_LIMIT", "600")),
        min_real_label_rows=int(os.getenv("MIN_REAL_LABEL_ROWS", "120")),
        real_labels_output_path=os.getenv("REAL_LABELS_OUTPUT_PATH", "data/labels/settled_labels.csv"),
        settled_archive_path=os.getenv("SETTLED_ARCHIVE_PATH", "data/labels/settled_markets_archive.json"),
        enable_live_execution=_as_bool(os.getenv("ENABLE_LIVE_EXECUTION", "false")),
        execution_mode=os.getenv("EXECUTION_MODE", "paper").strip().lower(),
        fee_rate_probability=float(os.getenv("FEE_RATE_PROBABILITY", "0.01")),
        slippage_probability=float(os.getenv("SLIPPAGE_PROBABILITY", "0.005")),
        bankroll_dollars=float(os.getenv("BANKROLL_DOLLARS", "5000")),
        max_exposure_dollars=float(os.getenv("MAX_EXPOSURE_DOLLARS", "800")),
        max_ticker_exposure_dollars=float(os.getenv("MAX_TICKER_EXPOSURE_DOLLARS", "250")),
        max_open_tickers=int(os.getenv("MAX_OPEN_TICKERS", "6")),
        max_trade_size_dollars=float(os.getenv("MAX_TRADE_SIZE_DOLLARS", "100")),
        daily_loss_limit_dollars=float(os.getenv("DAILY_LOSS_LIMIT_DOLLARS", "150")),
        kelly_fraction=float(os.getenv("KELLY_FRACTION", "0.20")),
        per_ticker_cooldown_seconds=int(os.getenv("PER_TICKER_COOLDOWN_SECONDS", "300")),
        min_market_volume=int(os.getenv("MIN_MARKET_VOLUME", "25")),
        max_spread_probability=float(os.getenv("MAX_SPREAD_PROBABILITY", "0.08")),
        min_expected_value_dollars_per_contract=float(
            os.getenv("MIN_EXPECTED_VALUE_DOLLARS_PER_CONTRACT", "0.015")
        ),
        max_alerts_per_scan=int(os.getenv("MAX_ALERTS_PER_SCAN", "10")),
        paper_trades_path=os.getenv("PAPER_TRADES_PATH", "data/paper_trades.jsonl"),
        paper_ledger_state_path=os.getenv("PAPER_LEDGER_STATE_PATH", "data/paper_ledger_state.json"),
        reconcile_settlements_in_paper=_as_bool(os.getenv("RECONCILE_SETTLEMENTS_IN_PAPER", "true")),
        paper_fill_probability=float(os.getenv("PAPER_FILL_PROBABILITY", "0.95")),
        paper_partial_fill_min_ratio=float(os.getenv("PAPER_PARTIAL_FILL_MIN_RATIO", "0.60")),
        paper_partial_fill_max_ratio=float(os.getenv("PAPER_PARTIAL_FILL_MAX_RATIO", "1.00")),
        paper_slippage_std_probability=float(os.getenv("PAPER_SLIPPAGE_STD_PROBABILITY", "0.01")),
        paper_fill_delay_ms_min=int(os.getenv("PAPER_FILL_DELAY_MS_MIN", "20")),
        paper_fill_delay_ms_max=int(os.getenv("PAPER_FILL_DELAY_MS_MAX", "250")),
        record_market_snapshots=_as_bool(os.getenv("RECORD_MARKET_SNAPSHOTS", "true")),
        market_snapshot_path=os.getenv("MARKET_SNAPSHOT_PATH", "data/raw/latest_markets.json"),
        replay_input_path=os.getenv("REPLAY_INPUT_PATH", "data/samples/sample_markets.json"),
        replay_settled_input_path=os.getenv("REPLAY_SETTLED_INPUT_PATH", "data/samples/sample_settled_markets.json"),
        live_market_limit=int(os.getenv("LIVE_MARKET_LIMIT", "200")),
        live_symbol_hints=os.getenv("LIVE_SYMBOL_HINTS", "SPY,QQQ,IWM,DIA,NDX,SPX"),
        diagnostics_report_path=os.getenv("DIAGNOSTICS_REPORT_PATH", "data/diagnostics/latest_report.json"),
        source_cascade_report_path=os.getenv("SOURCE_CASCADE_REPORT_PATH", "data/diagnostics/source_cascade_report.json"),
        paper_cycle_report_path=os.getenv("PAPER_CYCLE_REPORT_PATH", "data/diagnostics/paper_cycle_report.json"),
        monte_carlo_report_path=os.getenv("MONTE_CARLO_REPORT_PATH", "data/diagnostics/monte_carlo_report.json"),
        unrealized_pnl_report_path=os.getenv("UNREALIZED_PNL_REPORT_PATH", "data/diagnostics/unrealized_pnl_report.json"),
        settled_walk_forward_report_path=os.getenv("SETTLED_WALK_FORWARD_REPORT_PATH", "data/diagnostics/settled_walk_forward_report.json"),
        drift_report_path=os.getenv("DRIFT_REPORT_PATH", "data/diagnostics/drift_report.json"),
        health_dashboard_report_path=os.getenv("HEALTH_DASHBOARD_REPORT_PATH", "data/diagnostics/health_dashboard_report.json"),
        evidence_cycle_report_path=os.getenv("EVIDENCE_CYCLE_REPORT_PATH", "data/diagnostics/evidence_cycle_report.json"),
        recommendations_report_path=os.getenv("RECOMMENDATIONS_REPORT_PATH", "data/diagnostics/recommendations_report.json"),
        diagnostics_example_limit=int(os.getenv("DIAGNOSTICS_EXAMPLE_LIMIT", "5")),
        source_diagnostics_max_attempts=int(os.getenv("SOURCE_DIAGNOSTICS_MAX_ATTEMPTS", "30")),
        monte_carlo_trials=int(os.getenv("MONTE_CARLO_TRIALS", "2000")),
        monte_carlo_max_trades=int(os.getenv("MONTE_CARLO_MAX_TRADES", "1000")),
        monte_carlo_sample_sizes=os.getenv("MONTE_CARLO_SAMPLE_SIZES", "10,25,50,100,250,500,1000"),
        monte_carlo_seed=int(os.getenv("MONTE_CARLO_SEED", "42")),
        monte_carlo_min_settled_trades_for_grade=int(os.getenv("MONTE_CARLO_MIN_SETTLED_TRADES_FOR_GRADE", "50")),
        monte_carlo_health_pass_score=float(os.getenv("MONTE_CARLO_HEALTH_PASS_SCORE", "0.75")),
        monte_carlo_min_positive_prob_at_max_n=float(os.getenv("MONTE_CARLO_MIN_POSITIVE_PROB_AT_MAX_N", "0.60")),
        enforce_grade_gate_for_live=_as_bool(os.getenv("ENFORCE_GRADE_GATE_FOR_LIVE", "true")),
        settled_walk_forward_min_train_rows=int(os.getenv("SETTLED_WALK_FORWARD_MIN_TRAIN_ROWS", "200")),
        settled_walk_forward_test_rows=int(os.getenv("SETTLED_WALK_FORWARD_TEST_ROWS", "50")),
        drift_window_rows=int(os.getenv("DRIFT_WINDOW_ROWS", "100")),
        drift_brier_increase_threshold=float(os.getenv("DRIFT_BRIER_INCREASE_THRESHOLD", "0.03")),
        drift_ece_increase_threshold=float(os.getenv("DRIFT_ECE_INCREASE_THRESHOLD", "0.02")),
        drift_check_every_n_scans=int(os.getenv("DRIFT_CHECK_EVERY_N_SCANS", "20")),
        enable_drift_live_downgrade=_as_bool(os.getenv("ENABLE_DRIFT_LIVE_DOWNGRADE", "true")),
        recommendations_max_items=int(os.getenv("RECOMMENDATIONS_MAX_ITEMS", "12")),
        recommendations_min_confidence=float(os.getenv("RECOMMENDATIONS_MIN_CONFIDENCE", "0.55")),
        kalshi_api_base=os.getenv("KALSHI_API_BASE", "https://api.elections.kalshi.com"),
        kalshi_api_key=os.getenv("KALSHI_API_KEY"),
        kalshi_private_key_path=os.getenv("KALSHI_PRIVATE_KEY_PATH"),
    )
