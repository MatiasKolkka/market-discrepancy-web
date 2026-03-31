# Custom Domain Guide (Render)

This guide helps you move from the default Render URL to your own branded domain.

## 1) Create/Verify Render Service
- Connect GitHub repo: MatiasKolkka/market-discrepancy-web
- Confirm service deploys successfully.
- Confirm health endpoint works:
  - /health

## 2) Choose Final URL Structure
Common pattern:
- App: app.yourdomain.com
- Marketing site (optional): yourdomain.com

For this project, a clean choice is:
- app.marketdiscrepancy.com

## 3) Add Custom Domain In Render
- Open Render service.
- Go to Settings -> Custom Domains.
- Add your desired host (for example app.marketdiscrepancy.com).
- Render will show required DNS records.

## 4) Configure DNS At Your Registrar
Use exactly the records Render provides. Usually one of:
- CNAME record for subdomain (recommended)
- A/ALIAS records for apex/root domain

Typical subdomain setup:
- Type: CNAME
- Host/Name: app
- Value/Target: <render-assigned-domain>
- TTL: Auto

## 5) SSL/TLS
- Render provisions TLS automatically after DNS is correct.
- Wait for certificate status to become active.

## 6) Set Runtime Environment Variables
In Render service environment variables:
- ACTION_API_TOKEN=<strong secret>
- APP_DISPLAY_NAME=Market Scanner Signals
- PUBLIC_SITE_URL=https://app.marketdiscrepancy.com

## 7) Verify Endpoints
After DNS + SSL are active:
- https://app.marketdiscrepancy.com/
- https://app.marketdiscrepancy.com/health
- https://app.marketdiscrepancy.com/api/snapshot

## 8) Lock Down Actions
- Keep ACTION_API_TOKEN enabled.
- Do not expose token publicly.
- Share token only with trusted operators.

## 9) Post-Launch Checks
- Run a dashboard action and confirm output updates.
- Confirm recommendation scan button returns current suggestions.
- Confirm no-go/go state matches latest diagnostics.

## Troubleshooting
- Domain not resolving: DNS propagation can take time.
- SSL pending: wait for DNS correctness and recheck in Render.
- 401 on actions: token missing or incorrect.
- Empty recommendations: run scan-once/evidence-cycle first.
