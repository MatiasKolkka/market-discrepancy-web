# Market Discrepancy Scanner (Kalshi + Stocks)

This project scans event markets for probability discrepancies and can:
- alert you when edge exceeds a threshold
- optionally place orders (stubbed adapter, disabled by default)

V2 adds:
- trainable probability model (logistic + isotonic calibration)
- walk-forward validation
- net-edge filtering after cost assumptions
- confidence and risk-gated execution sizing

Latest upgrade adds:
- model artifact caching/loading to avoid cold-start retraining every run
- market quality gating (volume + spread filters)
- paper-trade journaling with simulated fills
- per-ticker cooldown, ticker exposure limits, and max open ticker controls
- explicit expected value floor per contract
- settled-market outcome labeling and training fallback logic
- unit tests for event parsing and risk rules

Current extension adds:
- direction-aware event parsing for above/below/range contracts
- market snapshot recording for offline diagnostics
- replay mode to scan from local JSON sample/live snapshots
- financial-template filtering to skip non-price contracts
- paper ledger settlement reconciliation for realized PnL lifecycle
- market diagnostics report mode to explain live/replay filtering bottlenecks
- live symbol-hint query cascade to prioritize finance-relevant contracts
- source-cascade diagnostics report with per-query conversion metrics
- paper-cycle mode that opens paper trades then reconciles settled outcomes
- Monte Carlo LLN validation mode to stress-test paper-cycle settlement PnL
- settled-data walk-forward validation report with regime breakdown
- calibration drift monitoring with optional auto downgrade to alert-only
- unrealized (mark-to-market) PnL report for open paper positions
- live safety gates tied to Monte Carlo grade and drift status
- realistic paper execution simulation (partial fills, slippage, delay)

## Important
This repo is for education and paper-testing. Do not use live execution until you fully validate strategy quality, risk controls, and exchange/broker requirements.

## What it does
1. Pulls market data from Kalshi-compatible endpoint.
2. Parses event text to extract symbol, strike, and approximate expiry horizon.
3. Builds a historical feature set from market data (volatility, momentum, moneyness).
4. Trains and calibrates a probability model.
5. Computes direction-aware probabilities (`above`, `below`, `range`) and net edge after fee/slippage/spread costs.
6. Filters low-quality markets by liquidity and spread.
7. Applies confidence + risk controls before execution.
8. Executes live only when explicitly enabled; otherwise journals paper trades.
9. Sends alerts and logs signals to `data/signals.jsonl`.
10. Reconciles paper positions against settled markets (optional) and realizes PnL.

Training data preference:
1. Tries real settled-market labels first.
2. Falls back to synthetic proxy labels if real rows are insufficient.

Real-label quality guardrails:
1. Requires explicit index symbol in event text (`SPY`, `QQQ`, `IWM`, `DIA`, `NDX`, `SPX`).
2. Requires price-like language (`close`, `price`, `settle`, `finish`, `end`) and a numeric threshold.

## Quick Start
1. Create and activate a virtual environment.
2. Install dependencies:

```powershell
pip install -r requirements.txt
```

3. Copy environment file:

```powershell
Copy-Item .env.example .env
```

4. Run one-shot scan:

```powershell
python src/main.py --mode scan-once
```

5. Run continuous scanning:

```powershell
python src/main.py --mode scan-loop
```

6. Run replay scan from local file:

```powershell
python src/main.py --mode scan-from-file --input-file data/samples/sample_markets.json
```

7. Generate diagnostics report (live or file input):

```powershell
python src/main.py --mode diagnose-markets
python src/main.py --mode diagnose-markets --input-file data/samples/sample_markets.json
```

8. Generate source-cascade diagnostics (query-family comparison):

```powershell
python src/main.py --mode diagnose-sources
python src/main.py --mode diagnose-sources --input-file data/samples/sample_markets.json
```

9. Run practical paper cycle test (open then settle):

```powershell
python src/main.py --mode paper-cycle --input-file data/samples/sample_markets.json --settled-file data/samples/sample_settled_markets.json
```

10. Run full paper validation suite (source diagnostics + paper cycle):

```powershell
python src/main.py --mode paper-cycle-suite --input-file data/samples/sample_markets.json --settled-file data/samples/sample_settled_markets.json
```

11. Run Monte Carlo + Law of Large Numbers validation on settlement PnL:

```powershell
python src/main.py --mode monte-carlo --input-file data/samples/sample_markets.json --settled-file data/samples/sample_settled_markets.json
```

12. Run Monte Carlo from your accumulated settled journal only (no replay reset):

```powershell
python src/main.py --mode monte-carlo-journal
```

13. Run settled-data walk-forward validation (file or live source):

```powershell
python src/main.py --mode settled-walk-forward --settled-file data/samples/sample_settled_markets.json
```

14. Run calibration drift monitor (file or live source):

```powershell
python src/main.py --mode drift-monitor --settled-file data/samples/sample_settled_markets.json
```

15. Build consolidated go/no-go health dashboard:

```powershell
python src/main.py --mode health-dashboard
python src/main.py --mode health-dashboard --refresh-drift --refresh-monte-carlo-journal --settled-file data/samples/sample_settled_markets.json
```

16. Run evidence cycle (append settled archive + refresh diagnostics + show remaining targets):

```powershell
python src/main.py --mode evidence-cycle
python src/main.py --mode evidence-cycle --settled-file data/samples/sample_settled_markets.json
```

17. Run walk-forward validation:

```powershell
python src/main.py --mode backtest
```

18. Build real settled-label dataset manually:

```powershell
python src/main.py --mode build-real-labels
```

19. Run tests:

```powershell
pytest
```

## Configuration
Set these values in `.env`:
- `MIN_EDGE_THRESHOLD`: minimum discrepancy to alert.
- `MIN_MODEL_CONFIDENCE`: confidence floor before trade consideration.
- `ALERT_WEBHOOK_URL`: optional webhook (Slack/Discord/custom).
- `EXECUTION_MODE`: `paper` or `live`.
- `ENABLE_LIVE_EXECUTION`: must be `true` to allow live mode.
- `FEE_RATE_PROBABILITY`, `SLIPPAGE_PROBABILITY`: cost assumptions.
- `MIN_MARKET_VOLUME`, `MAX_SPREAD_PROBABILITY`: market quality gates.
- `MIN_EXPECTED_VALUE_DOLLARS_PER_CONTRACT`: floor for contract expected value.
- `MAX_ALERTS_PER_SCAN`: caps noisy alert bursts.
- `BANKROLL_DOLLARS`, `MAX_EXPOSURE_DOLLARS`, `MAX_TICKER_EXPOSURE_DOLLARS`, `MAX_OPEN_TICKERS`, `MAX_TRADE_SIZE_DOLLARS`, `DAILY_LOSS_LIMIT_DOLLARS`, `PER_TICKER_COOLDOWN_SECONDS`: risk controls.
- `MODEL_ARTIFACT_PATH`, `MODEL_MAX_AGE_MINUTES`: model cache behavior.
- `PAPER_TRADES_PATH`: where paper fills are logged.
- `PAPER_LEDGER_STATE_PATH`, `RECONCILE_SETTLEMENTS_IN_PAPER`: paper lifecycle reconciliation controls.
- `PAPER_FILL_PROBABILITY`, `PAPER_PARTIAL_FILL_MIN_RATIO`, `PAPER_PARTIAL_FILL_MAX_RATIO`, `PAPER_SLIPPAGE_STD_PROBABILITY`, `PAPER_FILL_DELAY_MS_MIN`, `PAPER_FILL_DELAY_MS_MAX`: paper execution realism assumptions.
- `PREFER_REAL_OUTCOME_LABELS`, `SETTLED_MARKET_LIMIT`, `MIN_REAL_LABEL_ROWS`, `REAL_LABELS_OUTPUT_PATH`: real-outcome labeling controls.
- `RECORD_MARKET_SNAPSHOTS`, `MARKET_SNAPSHOT_PATH`: raw market snapshot capture.
- `REPLAY_INPUT_PATH`: default replay JSON file for `scan-from-file`.
- `REPLAY_SETTLED_INPUT_PATH`: default settled replay file for paper-cycle mode.
- `LIVE_MARKET_LIMIT`: number of markets requested in live fetch and diagnose mode.
- `LIVE_SYMBOL_HINTS`: comma-separated symbol priority list for live fallback queries.
- `DIAGNOSTICS_REPORT_PATH`, `DIAGNOSTICS_EXAMPLE_LIMIT`: market diagnostics report output and sample limits.
- `SOURCE_CASCADE_REPORT_PATH`, `SOURCE_DIAGNOSTICS_MAX_ATTEMPTS`: source-family diagnostics report settings.
- `PAPER_CYCLE_REPORT_PATH`: JSON artifact path for practical open->settle cycle summary.
- `MONTE_CARLO_REPORT_PATH`, `MONTE_CARLO_TRIALS`, `MONTE_CARLO_MAX_TRADES`, `MONTE_CARLO_SAMPLE_SIZES`, `MONTE_CARLO_SEED`: Monte Carlo/LLN simulation settings.
- `MONTE_CARLO_MIN_SETTLED_TRADES_FOR_GRADE`, `MONTE_CARLO_HEALTH_PASS_SCORE`, `MONTE_CARLO_MIN_POSITIVE_PROB_AT_MAX_N`: grading gate and pass/fail thresholds.
- `UNREALIZED_PNL_REPORT_PATH`: mark-to-market open-position report path.
- `SETTLED_WALK_FORWARD_REPORT_PATH`, `SETTLED_WALK_FORWARD_MIN_TRAIN_ROWS`, `SETTLED_WALK_FORWARD_TEST_ROWS`: settled walk-forward validation settings.
- `DRIFT_REPORT_PATH`, `DRIFT_WINDOW_ROWS`, `DRIFT_BRIER_INCREASE_THRESHOLD`, `DRIFT_ECE_INCREASE_THRESHOLD`, `DRIFT_CHECK_EVERY_N_SCANS`, `ENABLE_DRIFT_LIVE_DOWNGRADE`: drift monitor and auto-downgrade settings.
- `HEALTH_DASHBOARD_REPORT_PATH`: consolidated go/no-go decision report path.
- `SETTLED_ARCHIVE_PATH`: local deduped archive of normalized settled outcomes.
- `EVIDENCE_CYCLE_REPORT_PATH`: report path for end-to-end evidence pipeline status and remaining targets.
- `ENFORCE_GRADE_GATE_FOR_LIVE`: blocks live mode unless Monte Carlo grading status is `pass`.

Diagnostics metadata includes:
1. Fetch attempt cascade with selected params and error traces.
2. Payload profile counts for ticker/title/yes bid/ask fields.

Monte Carlo report includes:
1. LLN convergence table across sample sizes (`mean_of_means`, `std_of_means`, absolute error).
2. 95% bootstrap confidence interval for mean per-trade settled PnL.
3. Probability total PnL is positive for each sample size.
4. Low-sample warning when settled trade count is below 30.
5. Grading block with eligibility gate, check-by-check booleans, normalized health score, and status (`insufficient_data` | `pass` | `fail`).

Live execution safety gates include:
1. Grade gate: if enabled, live execution requires Monte Carlo grading status `pass`.
2. Drift gate: if enabled and drift is detected, execution is downgraded to alert-only.

Health dashboard decision checks include:
1. Monte Carlo grading status is `pass`.
2. Drift monitor does not detect calibration drift.
3. Settled walk-forward report is available and in `ok` status.

Live execution is blocked unless both of these are true:
- `EXECUTION_MODE=live`
- `ENABLE_LIVE_EXECUTION=true`

Even in live mode, risk controls can still block execution when constraints are violated.

## Known Drawbacks
1. Settled outcomes are used when available, but API field/endpoint changes can reduce usable rows and trigger fallback.
2. Fill model assumes immediate fills; queue position and partial fills are not modeled.
3. Settlement reconciliation depends on ticker matching and available resolved market fields.
4. Event parser is heuristic and may miss non-standard event wording.
5. Feature set is intentionally compact and does not yet include macro/news/event-driven factors.

## Next upgrades
1. Replace `BrokerExecutor` with authenticated order placement and idempotent order keys.
2. Add settled-market labeling pipeline and train on real event outcomes.
3. Add feature store (parquet/duckdb), model registry, and experiment tracking.
4. Build post-trade analytics (fill quality, realized edge capture, slippage decomposition).
5. Add calibration drift monitoring and automatic model rollback thresholds.
