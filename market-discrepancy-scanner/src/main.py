from __future__ import annotations

import argparse
from pathlib import Path

from dotenv import load_dotenv

from config import load_settings
from research.backtest import walk_forward_validate
from research.features import build_training_dataset
from scanner.engine import (
    build_and_export_real_labels,
    diagnose_fetch_sources,
    diagnose_markets,
    run_monte_carlo_validation_from_files,
    run_monte_carlo_validation_from_journal,
    run_calibration_drift_monitor,
    run_loop,
    run_once,
    run_once_from_file,
    run_paper_cycle_from_files,
    run_paper_cycle_suite,
    run_settled_walk_forward_validation,
    run_health_dashboard,
    run_evidence_cycle,
)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Market discrepancy scanner")
    parser.add_argument(
        "--mode",
        choices=[
            "scan-once",
            "scan-loop",
            "scan-from-file",
            "diagnose-markets",
            "diagnose-sources",
            "paper-cycle",
            "paper-cycle-suite",
            "monte-carlo",
            "monte-carlo-journal",
            "settled-walk-forward",
            "drift-monitor",
            "health-dashboard",
            "evidence-cycle",
            "backtest",
            "build-real-labels",
        ],
        default="scan-once",
        help="Operation mode",
    )
    parser.add_argument(
        "--input-file",
        default=None,
        help="Path to a JSON replay file containing markets (for scan-from-file mode)",
    )
    parser.add_argument(
        "--settled-file",
        default=None,
        help="Path to settled-market JSON for paper-cycle mode",
    )
    parser.add_argument(
        "--refresh-drift",
        action="store_true",
        help="Refresh drift report before building health dashboard",
    )
    parser.add_argument(
        "--refresh-monte-carlo-journal",
        action="store_true",
        help="Refresh Monte Carlo journal report before building health dashboard",
    )
    return parser


if __name__ == "__main__":
    load_dotenv()
    args = _build_parser().parse_args()
    settings = load_settings()
    log_file = Path("data/signals.jsonl")

    if args.mode == "scan-once":
        results = run_once(settings, log_file)
        print(f"One-shot scan complete. markets_scanned={len(results)}")
    elif args.mode == "scan-loop":
        run_loop(settings, log_file)
    elif args.mode == "scan-from-file":
        replay_path = args.input_file or settings.replay_input_path
        results = run_once_from_file(settings=settings, log_file=log_file, input_file=replay_path)
        print(
            "Replay scan complete"
            f" markets_scanned={len(results)}"
            f" input={replay_path}"
        )
    elif args.mode == "diagnose-markets":
        diagnostics = diagnose_markets(settings=settings, input_file=args.input_file)
        report = diagnostics.as_dict()
        print(
            "Diagnostics complete"
            f" scanned={report['scanned']}"
            f" blocked={report['blocked']}"
            f" passed={report['passed']}"
            f" report={settings.diagnostics_report_path}"
        )
    elif args.mode == "diagnose-sources":
        report = diagnose_fetch_sources(settings=settings, input_file=args.input_file)
        print(
            "Source diagnostics complete"
            f" attempts={report['attempt_count']}"
            f" best_pre_score_pass={report['best_pre_score_pass']}"
            f" report={settings.source_cascade_report_path}"
        )
    elif args.mode == "paper-cycle":
        replay_path = args.input_file or settings.replay_input_path
        settled_path = args.settled_file or settings.replay_settled_input_path
        summary = run_paper_cycle_from_files(
            settings=settings,
            log_file=log_file,
            market_file=replay_path,
            settled_file=settled_path,
            reset_state=True,
        )
        print(
            "Paper cycle complete"
            f" scanned={summary['markets_scanned']}"
            f" opened={summary['opened_paper_positions']}"
            f" closed={summary['reconciled_closed_positions']}"
            f" pnl={summary['reconciled_realized_pnl_dollars']:.2f}"
            f" bankroll={summary['portfolio_bankroll_dollars']:.2f}"
            f" report={settings.paper_cycle_report_path}"
        )
    elif args.mode == "paper-cycle-suite":
        replay_path = args.input_file or settings.replay_input_path
        settled_path = args.settled_file or settings.replay_settled_input_path
        summary = run_paper_cycle_suite(
            settings=settings,
            log_file=log_file,
            market_file=replay_path,
            settled_file=settled_path,
        )
        print(
            "Paper suite complete"
            f" best_pre_score_pass={summary['source_best_pre_score_pass']}"
            f" scanned={summary['markets_scanned']}"
            f" opened={summary['opened_paper_positions']}"
            f" closed={summary['reconciled_closed_positions']}"
            f" pnl={summary['reconciled_realized_pnl_dollars']:.2f}"
            f" source_report={summary['source_report_path']}"
            f" paper_report={summary['paper_cycle_report_path']}"
        )
    elif args.mode == "monte-carlo":
        replay_path = args.input_file or settings.replay_input_path
        settled_path = args.settled_file or settings.replay_settled_input_path
        summary = run_monte_carlo_validation_from_files(
            settings=settings,
            log_file=log_file,
            market_file=replay_path,
            settled_file=settled_path,
        )
        tail = summary["convergence"][-1]
        grade = summary["grading"]
        print(
            "Monte Carlo complete"
            f" trials={summary['trials']}"
            f" empirical_mean={summary['empirical_mean_trade_pnl']:.4f}"
            f" n={int(tail['sample_size'])}"
            f" mean_of_means={tail['mean_of_means']:.4f}"
            f" abs_error={tail['abs_error_vs_empirical_mean']:.4f}"
            f" grade_status={grade['status']}"
            f" grade_score={grade['health_score']:.2f}"
            f" report={settings.monte_carlo_report_path}"
        )
    elif args.mode == "monte-carlo-journal":
        summary = run_monte_carlo_validation_from_journal(settings)
        tail = summary["convergence"][-1]
        ci95 = summary["bootstrap_ci95_mean_trade_pnl"]
        grade = summary["grading"]
        print(
            "Monte Carlo journal complete"
            f" trades={summary['empirical_trade_count']}"
            f" empirical_mean={summary['empirical_mean_trade_pnl']:.4f}"
            f" ci95=[{ci95['lower']:.4f},{ci95['upper']:.4f}]"
            f" n={int(tail['sample_size'])}"
            f" p_pos={tail['prob_total_pnl_positive']:.3f}"
            f" grade_status={grade['status']}"
            f" grade_score={grade['health_score']:.2f}"
            f" report={settings.monte_carlo_report_path}"
        )
    elif args.mode == "settled-walk-forward":
        report = run_settled_walk_forward_validation(settings=settings, settled_file=args.settled_file)
        summary = report["summary"]
        print(
            "Settled walk-forward complete"
            f" folds={summary['fold_count']}"
            f" mean_brier={summary['mean_brier']:.4f}"
            f" mean_log_loss={summary['mean_log_loss']:.4f}"
            f" mean_ece={summary['mean_ece']:.4f}"
            f" report={settings.settled_walk_forward_report_path}"
        )
    elif args.mode == "drift-monitor":
        report = run_calibration_drift_monitor(settings=settings, settled_file=args.settled_file)
        print(
            "Drift monitor complete"
            f" drift_detected={report.get('drift_detected')}"
            f" recommended_mode={report.get('recommended_mode')}"
            f" report={settings.drift_report_path}"
        )
    elif args.mode == "health-dashboard":
        report = run_health_dashboard(
            settings=settings,
            refresh_drift=args.refresh_drift,
            refresh_monte_carlo_journal=args.refresh_monte_carlo_journal,
            settled_file=args.settled_file,
        )
        print(
            "Health dashboard complete"
            f" decision={report['decision']}"
            f" failures={len(report['failure_reasons'])}"
            f" report={settings.health_dashboard_report_path}"
        )
    elif args.mode == "evidence-cycle":
        report = run_evidence_cycle(settings=settings, settled_file=args.settled_file)
        print(
            "Evidence cycle complete"
            f" archive_rows={report['archive']['total_rows']}"
            f" added={report['archive']['added_rows']}"
            f" decision={report['health_dashboard']['decision']}"
            f" need_trades={report['next_targets']['additional_settled_trades_for_grade']}"
            f" need_wf_rows={report['next_targets']['additional_settled_rows_for_walk_forward']}"
            f" report={settings.evidence_cycle_report_path}"
        )
    elif args.mode == "build-real-labels":
        frame = build_and_export_real_labels(settings)
        print(
            "Real label dataset built"
            f" rows={len(frame)}"
            f" output={settings.real_labels_output_path}"
        )
    else:
        frame = build_training_dataset(
            symbol=settings.training_symbol,
            lookback_days=settings.training_lookback_days,
        )
        wf = walk_forward_validate(
            frame,
            min_train_rows=settings.min_training_rows,
            test_rows=200,
        )
        print(
            "Backtest complete"
            f" folds={wf.folds}"
            f" mean_brier={wf.mean_brier:.4f}"
            f" mean_log_loss={wf.mean_log_loss:.4f}"
        )
