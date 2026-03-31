const refreshBtn = document.getElementById("refresh-btn");
const snapshotNode = document.getElementById("snapshot-data");
const actionLog = document.getElementById("action-log");
const scanRecommendationsBtn = document.getElementById("scan-recommendations-btn");
const recommendationList = document.getElementById("recommendation-list");
const mathDetails = document.getElementById("math-details");
const budgetInput = document.getElementById("budget-input");
const applyBudgetBtn = document.getElementById("apply-budget-btn");
const budgetSummary = document.getElementById("budget-summary");
const tokenInput = document.getElementById("token-input");
const saveTokenBtn = document.getElementById("save-token-btn");
const clearTokenBtn = document.getElementById("clear-token-btn");
const authStatus = document.getElementById("auth-status");

let snapshot = {};
if (snapshotNode && snapshotNode.textContent) {
  try {
    snapshot = JSON.parse(snapshotNode.textContent);
  } catch (error) {
    console.error("Failed to parse snapshot JSON", error);
  }
}

if (refreshBtn) {
  refreshBtn.addEventListener("click", async () => {
    refreshBtn.setAttribute("disabled", "true");
    refreshBtn.textContent = "Refreshing...";

    try {
      const res = await fetch("/api/snapshot");
      if (!res.ok) {
        throw new Error(`HTTP ${res.status}`);
      }
      // Keep it simple for v1: hard reload to refresh all template-bound content.
      window.location.reload();
    } catch (error) {
      console.error(error);
      refreshBtn.textContent = "Retry Refresh";
      refreshBtn.removeAttribute("disabled");
    }
  });
}

const actionButtons = Array.from(document.querySelectorAll("[data-action]"));
const STORAGE_KEY = "market_scanner_action_token";
const BUDGET_STORAGE_KEY = "market_scanner_budget_dollars";

let tokenRequired = false;

const getStoredToken = () => localStorage.getItem(STORAGE_KEY) || "";

const clampBudget = (value) => {
  const amount = Number(value);
  if (!Number.isFinite(amount) || amount <= 0) {
    return Number((snapshot || {}).default_budget_dollars || 100);
  }
  return Math.max(1, Math.min(100000, Math.round(amount)));
};

const getBudgetValue = () => {
  const inputValue = budgetInput ? budgetInput.value : "";
  const stored = localStorage.getItem(BUDGET_STORAGE_KEY) || "";
  const raw = (inputValue || stored || String((snapshot || {}).default_budget_dollars || 100)).trim();
  const budget = clampBudget(raw);
  if (budgetInput) {
    budgetInput.value = String(budget);
  }
  localStorage.setItem(BUDGET_STORAGE_KEY, String(budget));
  return budget;
};

const setScanButtonState = (enabled, title = "") => {
  if (!scanRecommendationsBtn) {
    return;
  }
  if (enabled) {
    scanRecommendationsBtn.removeAttribute("disabled");
    scanRecommendationsBtn.removeAttribute("title");
  } else {
    scanRecommendationsBtn.setAttribute("disabled", "true");
    if (title) {
      scanRecommendationsBtn.setAttribute("title", title);
    }
  }
};

const setAuthStatusText = (text) => {
  if (authStatus) {
    authStatus.textContent = text;
  }
};

const updateActionButtons = (enabled) => {
  actionButtons.forEach((button) => {
    if (enabled) {
      button.removeAttribute("disabled");
    } else {
      button.setAttribute("disabled", "true");
    }
  });
};

const initTokenUi = async () => {
  const token = getStoredToken();
  if (tokenInput) {
    tokenInput.value = token;
  }

  try {
    const res = await fetch("/api/auth/status");
    const auth = await res.json();
    const required = Boolean(auth.token_required);
    tokenRequired = required;
    if (!required) {
      setAuthStatusText("Auth: open mode");
      updateActionButtons(true);
      setScanButtonState(scannerAvailable, scannerStatusMessage);
      return auth;
    }

    if (token) {
      setAuthStatusText("Auth: operator token loaded");
      updateActionButtons(true);
      setScanButtonState(scannerAvailable, scannerStatusMessage);
    } else {
      setAuthStatusText("Auth: token required");
      updateActionButtons(false);
      if (scannerAvailable) {
        setScanButtonState(false, "Operator token required to scan live markets.");
      }
    }
    return auth;
  } catch (error) {
    console.error(error);
    setAuthStatusText("Auth: status unavailable");
    return null;
  }
};

const writeActionLog = (message) => {
  if (!actionLog) {
    return;
  }
  actionLog.textContent = message;
};

const recommendationCard = (item) => `
  <article class="card recommendation">
    <p class="pill">${String(item.side || "").replace("buy_", "").toUpperCase()}</p>
    <h3>${item.event_name || item.ticker || "Unknown market"}</h3>
    <p class="metric">${item.quantity || 0} contracts</p>
    <p class="label">Estimated cost: $${Number(item.estimated_cost_dollars || 0).toFixed(2)}</p>
    <p class="hint">${item.instruction || "No instruction"}</p>
  </article>
`;

const recommendationMath = (item) => `
  <details>
    <summary>${item.ticker || "unknown"} - ${item.side || "unknown"}</summary>
    <ul>
      <li>Market probability: ${item.market_probability ?? "n/a"}</li>
      <li>Model probability: ${item.model_probability ?? "n/a"}</li>
      <li>Net edge: ${item.net_edge ?? "n/a"}</li>
      <li>Confidence: ${item.confidence ?? "n/a"}</li>
      <li>Estimated value: $${Number(item.estimated_value_dollars || 0).toFixed(2)}</li>
      <li>${(item.math || {}).edge_formula || ""}</li>
      <li>${(item.math || {}).cost_formula || ""}</li>
      <li>${(item.math || {}).ev_formula || ""}</li>
    </ul>
  </details>
`;

const renderRecommendations = (items) => {
  if (!Array.isArray(items) || !recommendationList || !mathDetails) {
    return;
  }
  if (items.length === 0) {
    recommendationList.innerHTML = '<article class="card"><p>No recommendations yet. Run a scan.</p></article>';
    mathDetails.innerHTML = "<p>No math details yet.</p>";
    return;
  }

  recommendationList.innerHTML = items.map(recommendationCard).join("\n");
  mathDetails.innerHTML = items.map(recommendationMath).join("\n");
};

const updateBudgetSummary = (recommendations) => {
  if (!budgetSummary || !recommendations) {
    return;
  }
  const budget = Number(recommendations.budget_dollars || getBudgetValue() || 0);
  const spent = Number(recommendations.budget_spent_dollars || 0);
  const remaining = Number(recommendations.budget_remaining_dollars || Math.max(0, budget - spent));
  budgetSummary.textContent = `Budget: $${budget.toFixed(2)} | Allocated: $${spent.toFixed(2)} | Remaining: $${remaining.toFixed(2)}`;
};

const scannerAvailable = Boolean((snapshot || {}).scanner_available);
const scannerStatusMessage = (snapshot || {}).scanner_status_message || "Live scanning is unavailable on this deployment.";

if (scanRecommendationsBtn && !scannerAvailable) {
  setScanButtonState(false, scannerStatusMessage);
}

const fetchRecommendations = async ({ runScan }) => {
  const budget = getBudgetValue();
  const token = getStoredToken();

  if (runScan) {
    const headers = { "Content-Type": "application/json" };
    if (token) {
      headers["X-Action-Token"] = token;
    }

    const response = await fetch("/api/recommendations/scan", {
      method: "POST",
      headers,
      body: JSON.stringify({ budget_dollars: budget }),
    });
    const payload = await response.json();
    if (!response.ok || !payload.ok) {
      throw new Error(payload.error || `HTTP ${response.status}`);
    }
    const recs = payload.recommendations || {};
    renderRecommendations(recs.items || []);
    updateBudgetSummary(recs);
    return;
  }

  const response = await fetch(`/api/recommendations?limit=6&budget_dollars=${encodeURIComponent(String(budget))}`);
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}`);
  }
  const recs = await response.json();
  renderRecommendations(recs.items || []);
  updateBudgetSummary(recs);
};

const runAction = async (action) => {
  writeActionLog(`Running ${action}...`);
  try {
    const token = getStoredToken();
    const headers = {
      "Content-Type": "application/json",
    };
    if (token) {
      headers["X-Action-Token"] = token;
    }

    const response = await fetch(`/api/run/${action}`, {
      method: "POST",
      headers,
      body: JSON.stringify({}),
    });
    const payload = await response.json();
    const body = [
      `Action: ${action}`,
      `OK: ${payload.ok}`,
      `Return code: ${payload.returncode ?? "n/a"}`,
      "",
      "STDOUT:",
      payload.stdout || "(empty)",
      "",
      "STDERR:",
      payload.stderr || "(empty)",
    ].join("\n");
    writeActionLog(body);
    if (payload.ok) {
      writeActionLog(`${body}\n\nSnapshot not auto-refreshed. Use Refresh Snapshot when ready.`);
    }
  } catch (error) {
    writeActionLog(`Action failed: ${error}`);
  }
};

if (saveTokenBtn) {
  saveTokenBtn.addEventListener("click", async () => {
    const token = tokenInput ? tokenInput.value.trim() : "";
    if (token) {
      localStorage.setItem(STORAGE_KEY, token);
      setAuthStatusText("Auth: token saved");
      updateActionButtons(true);
    } else {
      localStorage.removeItem(STORAGE_KEY);
      setAuthStatusText("Auth: token cleared");
      updateActionButtons(false);
    }
    await initTokenUi();
  });
}

if (clearTokenBtn) {
  clearTokenBtn.addEventListener("click", async () => {
    localStorage.removeItem(STORAGE_KEY);
    if (tokenInput) {
      tokenInput.value = "";
    }
    setAuthStatusText("Auth: token cleared");
    updateActionButtons(false);
    await initTokenUi();
  });
}

actionButtons.forEach((button) => {
  button.addEventListener("click", async () => {
    const action = button.getAttribute("data-action");
    if (!action) {
      return;
    }
    button.setAttribute("disabled", "true");
    try {
      await runAction(action);
    } finally {
      button.removeAttribute("disabled");
    }
  });
});

const renderCharts = () => {
  if (typeof Chart === "undefined") {
    return;
  }

  const chartData = snapshot.chart || {};
  const labels = chartData.labels || [];

  const llnCanvas = document.getElementById("lln-chart");
  if (llnCanvas) {
    new Chart(llnCanvas, {
      type: "line",
      data: {
        labels,
        datasets: [
          {
            label: "Abs Error vs Empirical Mean",
            data: chartData.abs_errors || [],
            borderColor: "#db5b36",
            backgroundColor: "rgba(219, 91, 54, 0.15)",
            tension: 0.28,
            fill: true,
          },
          {
            label: "Std of Means",
            data: chartData.std_means || [],
            borderColor: "#0f766e",
            backgroundColor: "rgba(15, 118, 110, 0.10)",
            tension: 0.28,
            fill: true,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { position: "bottom" },
        },
      },
    });
  }

  const riskCanvas = document.getElementById("risk-chart");
  if (riskCanvas) {
    new Chart(riskCanvas, {
      type: "bar",
      data: {
        labels: ["P(positive @ max n)", "WF mean brier", "WF mean ECE", "Drift brier delta", "Drift ECE delta"],
        datasets: [
          {
            label: "Current values",
            data: [
              (() => {
                const p = chartData.positive_probs || [];
                return p.length ? p[p.length - 1] : 0;
              })(),
              chartData.wf_mean_brier || 0,
              chartData.wf_mean_ece || 0,
              chartData.drift_brier_delta || 0,
              chartData.drift_ece_delta || 0,
            ],
            backgroundColor: [
              "rgba(15, 118, 110, 0.6)",
              "rgba(21, 32, 58, 0.6)",
              "rgba(21, 32, 58, 0.45)",
              "rgba(219, 91, 54, 0.6)",
              "rgba(219, 91, 54, 0.45)",
            ],
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { display: false },
        },
      },
    });
  }
};

renderCharts();
renderRecommendations((((snapshot || {}).recommendations || {}).items) || []);
updateBudgetSummary((snapshot || {}).recommendations || {});

if (applyBudgetBtn) {
  applyBudgetBtn.addEventListener("click", async () => {
    applyBudgetBtn.setAttribute("disabled", "true");
    try {
      await fetchRecommendations({ runScan: false });
      writeActionLog("Budget applied to latest recommendations.");
    } catch (error) {
      writeActionLog(`Failed applying budget: ${error}`);
    } finally {
      applyBudgetBtn.removeAttribute("disabled");
    }
  });
}

if (scanRecommendationsBtn) {
  scanRecommendationsBtn.addEventListener("click", async () => {
    if (!scannerAvailable) {
      writeActionLog(scannerStatusMessage);
      return;
    }
    if (tokenRequired && !getStoredToken()) {
      writeActionLog("Operator token required before running live scan.");
      return;
    }

    scanRecommendationsBtn.setAttribute("disabled", "true");
    writeActionLog("Running live scan for recommendations...");
    try {
      await fetchRecommendations({ runScan: true });
      writeActionLog("Scan complete. Recommendations updated.");
    } catch (error) {
      writeActionLog(`Recommendation scan failed: ${error}`);
    } finally {
      scanRecommendationsBtn.removeAttribute("disabled");
    }
  });
}

const initializePage = async () => {
  getBudgetValue();
  const auth = await initTokenUi();

  if (!scannerAvailable) {
    return;
  }

  if (auth && auth.token_required && !getStoredToken()) {
    writeActionLog("Live auto-scan skipped: save operator token to fetch fresh bets on load.");
    return;
  }

  try {
    writeActionLog("Running automatic live scan for fresh bets...");
    await fetchRecommendations({ runScan: true });
    writeActionLog("Auto-scan complete. Fresh bets loaded.");
  } catch (error) {
    writeActionLog(`Auto-scan failed: ${error}`);
  }
};

initializePage();
