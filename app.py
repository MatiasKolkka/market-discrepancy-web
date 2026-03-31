from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
from typing import Any

from flask import Flask, jsonify, render_template, request

BASE_DIR = Path(__file__).resolve().parent
SCANNER_DIR = BASE_DIR.parent / "market-discrepancy-scanner"
DIAG_DIR = SCANNER_DIR / "data" / "diagnostics"
DATA_DIR = SCANNER_DIR / "data"
SIGNALS_PATH = DATA_DIR / "signals.jsonl"
PAPER_TRADES_PATH = DATA_DIR / "paper_trades.jsonl"
APP_DISPLAY_NAME = os.getenv("APP_DISPLAY_NAME", "Market Scanner Signals")
PUBLIC_SITE_URL = os.getenv("PUBLIC_SITE_URL", "http://127.0.0.1:5050")

REPORT_FILES = {
    "health_dashboard": DIAG_DIR / "health_dashboard_report.json",
    "evidence_cycle": DIAG_DIR / "evidence_cycle_report.json",
    "monte_carlo": DIAG_DIR / "monte_carlo_report.json",
    "settled_walk_forward": DIAG_DIR / "settled_walk_forward_report.json",
    "drift": DIAG_DIR / "drift_report.json",
    "unrealized": DIAG_DIR / "unrealized_pnl_report.json",
}

ALLOWED_ACTIONS: dict[str, dict[str, Any]] = {
    "scan-once": {"mode": "scan-once", "flags": []},
    "evidence-cycle": {"mode": "evidence-cycle", "flags": []},
    "health-dashboard-refresh": {
        "mode": "health-dashboard",
        "flags": ["--refresh-drift", "--refresh-monte-carlo-journal"],
    },
    "drift-monitor": {"mode": "drift-monitor", "flags": []},
    "settled-walk-forward": {"mode": "settled-walk-forward", "flags": []},
    "monte-carlo-journal": {"mode": "monte-carlo-journal", "flags": []},
}


def _operator_token() -> str:
    return os.getenv("ACTION_API_TOKEN", "").strip()


def _auth_status() -> dict[str, Any]:
    required = bool(_operator_token())
    return {
        "token_required": required,
        "role": "operator" if required else "open",
    }


def _extract_token() -> str:
    header = request.headers.get("X-Action-Token", "").strip()
    if header:
        return header

    auth = request.headers.get("Authorization", "").strip()
    if auth.lower().startswith("bearer "):
        return auth[7:].strip()
    return ""


def _is_action_authorized() -> bool:
    expected = _operator_token()
    if not expected:
        return True
    supplied = _extract_token()
    return bool(supplied) and supplied == expected


def _scanner_python() -> str:
    configured = os.getenv("SCANNER_PYTHON", "").strip()
    if configured:
        return configured

    candidate = SCANNER_DIR.parent / ".venv" / "Scripts" / "python.exe"
    if candidate.exists():
        return str(candidate)

    return sys.executable


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _build_snapshot() -> dict[str, Any]:
    health = _read_json(REPORT_FILES["health_dashboard"])
    evidence = _read_json(REPORT_FILES["evidence_cycle"])
    monte = _read_json(REPORT_FILES["monte_carlo"])
    walk = _read_json(REPORT_FILES["settled_walk_forward"])
    drift = _read_json(REPORT_FILES["drift"])
    unrealized = _read_json(REPORT_FILES["unrealized"])

    checks = health.get("checks") if isinstance(health.get("checks"), dict) else {}
    convergence = monte.get("convergence") if isinstance(monte.get("convergence"), list) else []
    labels: list[str] = []
    abs_errors: list[float] = []
    std_means: list[float] = []
    pos_probs: list[float] = []
    for row in convergence:
        if not isinstance(row, dict):
            continue
        labels.append(str(int(float(row.get("sample_size", 0)))))
        abs_errors.append(float(row.get("abs_error_vs_empirical_mean", 0.0)))
        std_means.append(float(row.get("std_of_means", 0.0)))
        pos_probs.append(float(row.get("prob_total_pnl_positive", 0.0)))

    drift_delta = drift.get("delta") if isinstance(drift.get("delta"), dict) else {}
    wf_summary = walk.get("summary") if isinstance(walk.get("summary"), dict) else {}

    snapshot = {
        "app_display_name": APP_DISPLAY_NAME,
        "public_site_url": PUBLIC_SITE_URL,
        "decision": health.get("decision", "unknown"),
        "failure_reasons": health.get("failure_reasons", []),
        "grade_status": (
            (monte.get("grading") or {}).get("status")
            if isinstance(monte.get("grading"), dict)
            else "missing"
        ),
        "grade_score": (
            (monte.get("grading") or {}).get("health_score")
            if isinstance(monte.get("grading"), dict)
            else None
        ),
        "drift_detected": drift.get("drift_detected"),
        "walk_forward_status": walk.get("status", "missing"),
        "open_positions": unrealized.get("open_positions", 0),
        "total_unrealized_pnl": unrealized.get("total_unrealized_pnl_dollars", 0.0),
        "empirical_trade_count": monte.get("empirical_trade_count", 0),
        "needed_trades": ((evidence.get("next_targets") or {}).get("additional_settled_trades_for_grade", None)),
        "needed_wf_rows": ((evidence.get("next_targets") or {}).get("additional_settled_rows_for_walk_forward", None)),
        "checks": checks,
        "chart": {
            "labels": labels,
            "abs_errors": abs_errors,
            "std_means": std_means,
            "positive_probs": pos_probs,
            "drift_brier_delta": float(drift_delta.get("brier", 0.0) or 0.0),
            "drift_ece_delta": float(drift_delta.get("ece", 0.0) or 0.0),
            "wf_mean_brier": float(wf_summary.get("mean_brier", 0.0) or 0.0),
            "wf_mean_ece": float(wf_summary.get("mean_ece", 0.0) or 0.0),
        },
        "raw": {
            "health": health,
            "evidence": evidence,
            "monte": monte,
            "walk": walk,
            "drift": drift,
            "unrealized": unrealized,
        },
    }
    return snapshot


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except Exception:
            continue
        if isinstance(row, dict):
            rows.append(row)
    return rows


def _build_recommendations(limit: int = 8) -> dict[str, Any]:
    signal_rows = _read_jsonl(SIGNALS_PATH)
    trade_rows = _read_jsonl(PAPER_TRADES_PATH)

    latest_signal_by_ticker: dict[str, dict[str, Any]] = {}
    for row in signal_rows:
        ticker = str(row.get("ticker") or "").strip()
        if not ticker:
            continue
        latest_signal_by_ticker[ticker] = row

    candidates: list[dict[str, Any]] = []
    for row in trade_rows:
        if row.get("mode") != "paper":
            continue
        if row.get("event_type") is not None:
            continue

        qty = int(row.get("quantity", 0) or 0)
        if qty <= 0:
            continue

        ticker = str(row.get("ticker") or "")
        side = str(row.get("side") or "")
        market_p = float(row.get("market_probability", 0.0) or 0.0)
        model_p = float(row.get("model_probability", 0.0) or 0.0)
        net_edge = float(row.get("net_edge", 0.0) or 0.0)
        confidence = float(row.get("confidence", 0.0) or 0.0)
        ts = str(row.get("timestamp_utc") or "")

        price = market_p if side == "buy_yes" else (1.0 - market_p)
        est_cost = max(0.0, qty * price)
        est_ev = abs(net_edge) * qty
        event_name = str((latest_signal_by_ticker.get(ticker) or {}).get("event_name") or ticker)

        if side == "buy_yes":
            instruction = f"Bet YES on {event_name}"
        else:
            instruction = f"Bet NO on {event_name}"

        candidates.append(
            {
                "ticker": ticker,
                "event_name": event_name,
                "instruction": instruction,
                "side": side,
                "quantity": qty,
                "estimated_cost_dollars": round(est_cost, 2),
                "model_probability": round(model_p, 4),
                "market_probability": round(market_p, 4),
                "net_edge": round(net_edge, 4),
                "confidence": round(confidence, 4),
                "estimated_value_dollars": round(est_ev, 2),
                "timestamp_utc": ts,
                "math": {
                    "edge_formula": "net_edge = (model_probability - market_probability) - costs",
                    "cost_formula": "estimated_cost = quantity * entry_price",
                    "ev_formula": "estimated_value = |net_edge| * quantity",
                },
            }
        )

    candidates.sort(key=lambda item: (item["timestamp_utc"], abs(float(item["net_edge"]))), reverse=True)
    recs = candidates[: max(1, limit)]
    return {
        "count": len(recs),
        "items": recs,
        "source": {
            "signals_path": str(SIGNALS_PATH.relative_to(SCANNER_DIR)),
            "paper_trades_path": str(PAPER_TRADES_PATH.relative_to(SCANNER_DIR)),
        },
    }


def _run_scanner_action(action: str, settled_file: str | None = None) -> dict[str, Any]:
    if action not in ALLOWED_ACTIONS:
        return {"ok": False, "error": f"Unsupported action: {action}"}

    spec = ALLOWED_ACTIONS[action]
    mode = str(spec["mode"])
    flags = [str(item) for item in spec.get("flags", [])]

    cmd = [_scanner_python(), "src/main.py", "--mode", mode, *flags]
    if settled_file:
        cmd.extend(["--settled-file", settled_file])

    proc = subprocess.run(
        cmd,
        cwd=str(SCANNER_DIR),
        text=True,
        capture_output=True,
        timeout=300,
        check=False,
    )
    return {
        "ok": proc.returncode == 0,
        "returncode": proc.returncode,
        "command": cmd,
        "stdout": proc.stdout[-8000:],
        "stderr": proc.stderr[-4000:],
    }


app = Flask(__name__)


@app.get("/")
def index() -> str:
    data = _build_snapshot()
    data["recommendations"] = _build_recommendations(limit=6)
    data["app_display_name"] = APP_DISPLAY_NAME
    data["public_site_url"] = PUBLIC_SITE_URL
    return render_template("index.html", data=data)


@app.get("/api/snapshot")
def api_snapshot() -> Any:
    out = _build_snapshot()
    out["recommendations"] = _build_recommendations(limit=6)
    return jsonify(out)


@app.get("/api/actions")
def api_actions() -> Any:
    return jsonify({"actions": sorted(ALLOWED_ACTIONS.keys()), "auth": _auth_status()})


@app.get("/api/auth/status")
def api_auth_status() -> Any:
    return jsonify(_auth_status())


@app.post("/api/run/<action>")
def api_run_action(action: str) -> Any:
    if not _is_action_authorized():
        return jsonify({"ok": False, "error": "Unauthorized action request"}), 401

    payload = request.get_json(silent=True)
    settled_file = None
    if isinstance(payload, dict):
        raw = payload.get("settled_file")
        if isinstance(raw, str) and raw.strip():
            settled_file = raw.strip()

    result = _run_scanner_action(action=action, settled_file=settled_file)
    status = 200 if result.get("ok") else 400
    return jsonify(result), status


@app.get("/api/recommendations")
def api_recommendations() -> Any:
    limit_raw = request.args.get("limit", "6")
    try:
        limit = max(1, min(20, int(limit_raw)))
    except ValueError:
        limit = 6
    return jsonify(_build_recommendations(limit=limit))


@app.post("/api/recommendations/scan")
def api_recommendations_scan() -> Any:
    if not _is_action_authorized():
        return jsonify({"ok": False, "error": "Unauthorized action request"}), 401

    run = _run_scanner_action(action="scan-once")
    status = 200 if run.get("ok") else 400
    payload = {
        "ok": bool(run.get("ok")),
        "run": run,
        "recommendations": _build_recommendations(limit=6),
    }
    return jsonify(payload), status


if __name__ == "__main__":
    app.run(debug=True, port=5050)
