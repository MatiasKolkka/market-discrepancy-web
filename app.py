from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
import subprocess
import sys
from typing import Any

from flask import Flask, jsonify, render_template, request

BASE_DIR = Path(__file__).resolve().parent


def _resolve_scanner_dir() -> Path:
    configured = os.getenv("SCANNER_DIR", "").strip()
    if configured:
        return Path(configured).expanduser().resolve()

    candidates = [
        BASE_DIR.parent / "market-discrepancy-scanner",
        BASE_DIR / "market-discrepancy-scanner",
    ]
    for path in candidates:
        if path.exists():
            return path
    return candidates[0]


SCANNER_DIR = _resolve_scanner_dir()
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
    "recommendations": DIAG_DIR / "recommendations_report.json",
}
RECOMMENDATIONS_MAX_AGE_MINUTES = int(os.getenv("RECOMMENDATIONS_MAX_AGE_MINUTES", "240"))
DEFAULT_BUDGET_DOLLARS = float(os.getenv("DEFAULT_BUDGET_DOLLARS", "100"))

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


def _scanner_coverage_hints() -> list[str]:
    raw = os.getenv(
        "LIVE_TOPIC_HINTS",
        "NFL,NBA,MLB,NHL,Oscars,Grammy,Box Office,Bitcoin,Election,AI",
    )
    hints = [token.strip() for token in raw.split(",") if token.strip()]
    return hints[:12]


def _minutes_since_iso(ts: str) -> float | None:
    try:
        stamp = datetime.fromisoformat(ts)
    except Exception:
        return None
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=timezone.utc)
    age = datetime.now(timezone.utc) - stamp.astimezone(timezone.utc)
    return max(0.0, age.total_seconds() / 60.0)


def _parse_budget(raw: str | None, default: float | None = None) -> float | None:
    if raw is None:
        return default
    text = str(raw).strip()
    if not text:
        return default
    try:
        amount = float(text)
    except ValueError:
        return default
    if amount <= 0:
        return default
    return max(1.0, min(100000.0, amount))


def _apply_budget(items: list[dict[str, Any]], budget_dollars: float | None) -> tuple[list[dict[str, Any]], float]:
    if budget_dollars is None:
        return items, sum(float(item.get("estimated_cost_dollars", 0.0) or 0.0) for item in items)

    remaining = float(budget_dollars)
    spent = 0.0
    budgeted: list[dict[str, Any]] = []

    for item in items:
        qty = int(item.get("quantity", 0) or 0)
        est_cost = float(item.get("estimated_cost_dollars", 0.0) or 0.0)
        if qty <= 0 or est_cost <= 0.0:
            continue

        per_contract = est_cost / max(1, qty)
        max_affordable = int(remaining // max(0.0001, per_contract))
        if max_affordable <= 0:
            continue

        new_qty = min(qty, max_affordable)
        scale = float(new_qty) / float(qty)
        new_cost = round(per_contract * new_qty, 2)

        updated = dict(item)
        updated["quantity"] = new_qty
        updated["estimated_cost_dollars"] = new_cost
        updated["estimated_value_dollars"] = round(float(item.get("estimated_value_dollars", 0.0) or 0.0) * scale, 2)
        budgeted.append(updated)

        spent += new_cost
        remaining = max(0.0, remaining - new_cost)
        if remaining < per_contract:
            break

    return budgeted, round(spent, 2)


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

    scanner_available = SCANNER_DIR.exists() and (SCANNER_DIR / "src" / "main.py").exists()

    snapshot = {
        "app_display_name": APP_DISPLAY_NAME,
        "public_site_url": PUBLIC_SITE_URL,
        "scanner_available": scanner_available,
        "coverage_hints": _scanner_coverage_hints(),
        "scanner_status_message": (
            "Live scanning unavailable on this deployment. "
            "This web service does not include the scanner backend folder."
            if not scanner_available
            else "Live scanner backend is available."
        ),
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


def _build_recommendations_from_logs(limit: int = 8) -> dict[str, Any]:
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
        "generated_at_utc": None,
        "is_fresh": False,
        "source_kind": "log_fallback",
        "source": {
            "signals_path": str(SIGNALS_PATH.relative_to(SCANNER_DIR)),
            "paper_trades_path": str(PAPER_TRADES_PATH.relative_to(SCANNER_DIR)),
        },
    }


def _build_recommendations(limit: int = 8, budget_dollars: float | None = None) -> dict[str, Any]:
    report = _read_json(REPORT_FILES["recommendations"])
    if report:
        items = report.get("items") if isinstance(report.get("items"), list) else []
        generated = str(report.get("generated_at_utc") or "")
        age_mins = _minutes_since_iso(generated) if generated else None
        is_fresh = bool(age_mins is not None and age_mins <= max(1, RECOMMENDATIONS_MAX_AGE_MINUTES))
        normalized: list[dict[str, Any]] = []
        for row in items[: max(1, limit)]:
            if not isinstance(row, dict):
                continue
            normalized.append(row)

        budgeted, spent = _apply_budget(normalized, budget_dollars)
        budget_value = budget_dollars if budget_dollars is not None else spent
        return {
            "count": len(budgeted),
            "items": budgeted,
            "generated_at_utc": generated,
            "age_minutes": age_mins,
            "is_fresh": is_fresh,
            "budget_dollars": round(float(budget_value), 2),
            "budget_spent_dollars": spent,
            "budget_remaining_dollars": round(max(0.0, float(budget_value) - spent), 2),
            "source_kind": "recommendations_report",
            "source": {
                "recommendations_path": str(REPORT_FILES["recommendations"].relative_to(SCANNER_DIR)),
            },
        }

    fallback = _build_recommendations_from_logs(limit=limit)
    fallback_items = fallback.get("items") if isinstance(fallback.get("items"), list) else []
    budgeted, spent = _apply_budget(fallback_items, budget_dollars)
    budget_value = budget_dollars if budget_dollars is not None else spent
    fallback["items"] = budgeted
    fallback["count"] = len(budgeted)
    fallback["budget_dollars"] = round(float(budget_value), 2)
    fallback["budget_spent_dollars"] = spent
    fallback["budget_remaining_dollars"] = round(max(0.0, float(budget_value) - spent), 2)
    return fallback


def _run_scanner_action(action: str, settled_file: str | None = None) -> dict[str, Any]:
    if action not in ALLOWED_ACTIONS:
        return {"ok": False, "error": f"Unsupported action: {action}"}

    if not SCANNER_DIR.exists():
        return {
            "ok": False,
            "error": (
                "Scanner backend directory not found. "
                "Deploy with both market-discrepancy-web and market-discrepancy-scanner folders "
                "or provide diagnostics report files to this service."
            ),
            "scanner_dir": str(SCANNER_DIR),
        }

    spec = ALLOWED_ACTIONS[action]
    mode = str(spec["mode"])
    flags = [str(item) for item in spec.get("flags", [])]

    cmd = [_scanner_python(), "src/main.py", "--mode", mode, *flags]
    if settled_file:
        cmd.extend(["--settled-file", settled_file])

    try:
        proc = subprocess.run(
            cmd,
            cwd=str(SCANNER_DIR),
            text=True,
            capture_output=True,
            timeout=300,
            check=False,
        )
    except Exception as exc:
        return {
            "ok": False,
            "error": f"Failed to run scanner action: {exc}",
            "command": cmd,
        }
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
    budget = _parse_budget(request.args.get("budget_dollars"), default=DEFAULT_BUDGET_DOLLARS)
    data["default_budget_dollars"] = round(float(budget or DEFAULT_BUDGET_DOLLARS), 2)
    data["recommendations"] = _build_recommendations(limit=6, budget_dollars=budget)
    data["app_display_name"] = APP_DISPLAY_NAME
    data["public_site_url"] = PUBLIC_SITE_URL
    return render_template("index.html", data=data)


@app.get("/api/snapshot")
def api_snapshot() -> Any:
    out = _build_snapshot()
    budget = _parse_budget(request.args.get("budget_dollars"), default=DEFAULT_BUDGET_DOLLARS)
    out["default_budget_dollars"] = round(float(budget or DEFAULT_BUDGET_DOLLARS), 2)
    out["recommendations"] = _build_recommendations(limit=6, budget_dollars=budget)
    return jsonify(out)


@app.get("/health")
def health() -> Any:
    return jsonify({"ok": True, "service": APP_DISPLAY_NAME}), 200


@app.get("/api/health")
def api_health() -> Any:
    return jsonify({"ok": True, "service": APP_DISPLAY_NAME}), 200


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
    budget = _parse_budget(request.args.get("budget_dollars"), default=DEFAULT_BUDGET_DOLLARS)
    return jsonify(_build_recommendations(limit=limit, budget_dollars=budget))


@app.post("/api/recommendations/scan")
def api_recommendations_scan() -> Any:
    if not _is_action_authorized():
        return jsonify({"ok": False, "error": "Unauthorized action request"}), 401

    payload = request.get_json(silent=True)
    budget = None
    if isinstance(payload, dict):
        budget = _parse_budget(payload.get("budget_dollars"), default=None)
    if budget is None:
        budget = DEFAULT_BUDGET_DOLLARS

    run = _run_scanner_action(action="scan-once")
    status = 200 if run.get("ok") else 400
    payload = {
        "ok": bool(run.get("ok")),
        "run": run,
        "recommendations": _build_recommendations(limit=6, budget_dollars=budget),
    }
    return jsonify(payload), status


if __name__ == "__main__":
    app.run(debug=True, port=5050)
