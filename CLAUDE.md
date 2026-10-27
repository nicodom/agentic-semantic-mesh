# agentic-semantic-mesh

AI agents over real-time event streams, grounded in a semantic data mesh. Read `README.md`, `docs/scope.md` and `docs/adr/README.md` first; the current milestone plan is in `docs/plan/`.

## Why this repo exists

The owner is an engineering leader using this project to build hands-on depth in LLM and agent engineering, and to explain the trade-offs in interviews and talks. So:

- **Explain as you build.** When introducing a concept, library or pattern the owner may not have used (Temporal determinism, MCP, OTel GenAI conventions, pgvector, tool calling), say briefly what it is and why it is used here.
- **Prefer understanding over speed.** No agent frameworks (LangChain, LangGraph, CrewAI). Agent loops and tool calling are written directly against the provider SDKs.
- **Quality over deadlines.** There are no fixed dates; do not cut scope to hit one.

## Decisions

- Every significant decision gets an ADR in `docs/adr/` (format in ADR 0001), written when the decision is made, and listed in `docs/adr/README.md`.
- Never edit an accepted ADR. A change of mind is a new ADR that supersedes it; mark the old one `Superseded by N`.
- Before changing anything an ADR decided, point to the ADR and ask.

## Conventions

- Python, managed with `uv`. Type hints throughout; `ruff` for lint and format; `pytest` for tests and evals.
- Stream processing in Flink SQL (no JVM code).
- Everything runs locally via `docker compose`; no service may require a paid cloud account.
- LLM access only through the provider interface (Claude, OpenAI, local via Ollama). Default to the local model for development runs.
- Secrets live in `.env` (gitignored); `.env.example` lists every variable with no values.

## Cost

Paid LLM APIs are capped at EUR 50/month (ADR 0012). Before running anything that calls a paid model in a loop or over a full eval suite, state the expected number of calls and roughly what they cost, and use the local model unless the run is a deliberate comparison.

## Commits

Small commits with clear messages. The repo is public: never commit secrets, personal data, or anything from outside this folder.
