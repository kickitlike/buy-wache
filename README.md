# BUY-WACHE

Experimental open-source crypto market scanner and execution-aware signal-audit prototype.

## Status

This repository currently contains a **V26.3 candidate implementation** extracted from a development/test artifact. It is not the active production scanner and has not been independently reproduced or validated from this repository. Do not deploy it to a live trading environment.

The code:
- evaluates a fixed 55-symbol USDT universe;
- uses closed one-minute candles and higher-timeframe context;
- separates early candidates, confirmed breakouts, watch states and failed breakouts;
- emits heuristic ranking scores, **not calibrated probabilities**;
- estimates entry, stop and target levels using configurable cost assumptions;
- records observations, predictions and outcome evaluations through Supabase REST.

## Important limitations

- Estimated fees, spread and slippage are assumptions, not live execution quotes.
- The code's own audit fields must be reviewed; a successful HTTP response is not proof of profitable or executable trades.
- The candidate was previously marked as a test-project replacement candidate, not production-deployed code.
- No trading performance, user count, downloads or community adoption is claimed.

## Configuration

The function reads these values from its runtime environment:
- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`
- `BUY_WACHE_INVOKE_SECRET`
- `BUY_WACHE_FEE_BPS_PER_SIDE` (default 10)
- `BUY_WACHE_SPREAD_BPS_TOTAL` (default 5)
- `BUY_WACHE_SLIPPAGE_BPS_PER_SIDE` (default 5)

Never commit credentials, API keys, private market data or personal account information. Use a dedicated test Supabase project and review the database schema/RPC dependencies before attempting to run the function.

## License

MIT. See [LICENSE](LICENSE).
