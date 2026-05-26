# Examples

Worked examples that exercise `instructed` end-to-end. Organised
by SDK language (TODO #8); cross-language scenarios will live
under `mixed/` once a second SDK lands.

| Example | Language | What it shows |
|---|---|---|
| [`typescript/bank-account/`](typescript/bank-account/) | TypeScript | Two aggregates, two projections, one process manager. Transfers with explicit success/failure outcome events. Each component runnable in its own process; isolated docker-compose. |

Each example ships its own `docker-compose.yaml` and `npm start`
so it can be run without interfering with the repo-root test
database. Follow the example's own README for the precise
one-liner.
