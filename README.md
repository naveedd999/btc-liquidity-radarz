# BTC Liquidity Radar — single-service deployment build

A BTC/USDT-only market microstructure dashboard. The container runs the Next.js frontend, FastAPI backend, and Nginx reverse proxy together behind one public service/port.

## Deploy to SnapDeploy
1. Put the **contents of this folder** at the root of your GitHub repository (so `Dockerfile`, `package.json`, `app/`, `backend/`, and `deploy/` are visible at repository root).
2. In SnapDeploy, create a deployment from that GitHub repository and select **Dockerfile / Docker deployment** if asked for a framework.
3. Set the health-check path to `/health` if the platform asks.
4. Set `DB_PATH` to `/data/radar.db` if environment variables are required. The container defaults to `PORT=8080`; the startup command also honours the platform-provided `PORT`.
5. Deploy, open the assigned URL, and wait for Binance Futures data to synchronize. The dashboard intentionally displays dashes while verified data is unavailable.
6. Test `https://YOUR-APP-URL/health`; it should return a JSON health response. The dashboard's WebSocket uses the same public host.

## Important hosting notes
- SnapDeploy free containers sleep when idle and their local filesystem can reset. A sleeping container is not a continuously running market monitor; live data resumes after wake-up and re-synchronization.
- For reliable always-on WebSockets, use an Always-On plan or another host that supports persistent WebSocket connections.
- SQLite history is stored at `/data/radar.db`; it is only persistent if the host provides persistent storage mounted at `/data`.
- The app uses public Binance Futures market data and does not require API keys for the configured public endpoints.
- Analytical indicators are heuristics, not financial advice or proof of manipulation.

## Local run
Docker build/run is the recommended way to run the complete stack:

```bash
docker build -t btc-liquidity-radar .
docker run --rm -p 8080:8080 -e PORT=8080 -v radar-data:/data btc-liquidity-radar
```
