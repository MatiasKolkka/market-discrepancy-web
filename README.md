# Market Discrepancy Web Dashboard

This is a separate web project that visualizes diagnostics from the scanner backend.

## Folder Layout
- Backend engine: `../market-discrepancy-scanner`
- Web dashboard: this folder (`market-discrepancy-web`)

## What it reads
By default, the app reads JSON report files from:
- `../market-discrepancy-scanner/data/diagnostics`

## New Features
- Run backend scanner workflows directly from dashboard action buttons.
- View LLN and risk/stability charts built from diagnostics.
- Use production WSGI serving via Waitress.
- Recommendation-first UX: plain-language "what to bet" cards with suggested contract quantity and cost.
- Optional math section at the bottom for advanced users.

## Quick Start
1. Create and activate a Python environment (or reuse one).
2. Install dependencies:

```powershell
pip install -r requirements.txt
```

3. Run the app:

```powershell
python app.py
```

4. Open:
- `http://127.0.0.1:5050`

## API
- `GET /api/snapshot`: full dashboard snapshot payload.
- `GET /api/actions`: supported backend actions.
- `GET /api/auth/status`: auth requirement for action execution.
- `POST /api/run/<action>`: run an allowed scanner action.
- `GET /api/recommendations`: current recommendation list.
- `POST /api/recommendations/scan`: run a live scan and return refreshed recommendations.

Supported actions:
- `scan-once`
- `evidence-cycle`
- `health-dashboard-refresh`
- `drift-monitor`
- `settled-walk-forward`
- `monte-carlo-journal`

Example:

```powershell
Invoke-RestMethod -Method Post -Uri "http://127.0.0.1:5050/api/run/evidence-cycle" -Body "{}" -ContentType "application/json"
```

## Production Run (Windows)
```powershell
.\start-prod.ps1
```

## Connect To GitHub
From this folder, run:

```powershell
git init
git add .
git commit -m "Initial web dashboard"
git branch -M main
git remote add origin https://github.com/<your-user>/<your-repo>.git
git push -u origin main
```

## Deploy From GitHub And Set URL Name
Recommended host: Render (supports Flask + custom domains).

1. Create a new Render Web Service from your GitHub repo.
2. Render will detect `render.yaml` in this project.
3. Set environment variables in Render:
	- `ACTION_API_TOKEN`
	- `APP_DISPLAY_NAME` (brand users see)
	- `PUBLIC_SITE_URL` (your final domain)
4. In Render, open `Settings -> Custom Domains` and add your domain.
5. In your DNS provider, add the CNAME/A record Render gives you.
6. Wait for SSL to issue; then your custom URL name is live.

## Auth (Recommended)
Set an operator token to protect action endpoints:

```powershell
$env:ACTION_API_TOKEN = "your-strong-token"
python app.py
```

Then paste the same token into the dashboard token field to unlock action buttons.

## Docker
```powershell
docker build -t market-discrepancy-web .
docker run --rm -p 5050:5050 market-discrepancy-web
```

## Notes
- The dashboard reads backend JSON files and can trigger selected backend modes.
- Ensure scanner reports exist or run `evidence-cycle` from the dashboard first.
