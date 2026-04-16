# @lbruton/claude-code-session-rag

> Actively maintained fork of [mwgreen/claude-code-session-rag](https://github.com/mwgreen/claude-code-session-rag) — hardened for production use with reliable backfill, project-root tagging, and date-range filtering.

## Why this fork?

The upstream `claude-code-session-rag` provides excellent foundations — semantic search over Claude Code transcripts, real-time file watching, and hybrid FTS+vector search. When used at scale (10K+ transcripts, 20+ projects), several issues surface:

- **Empty `project_root` on most rows** — transcripts in generic slug directories get `project_root=""`, making project-scoped search return nothing
- **Backfill crashes silently** — Milvus-lite gRPC sends `GOAWAY`/`too_many_pings` under heavy insert load, killing the backfill with no recovery
- **Backfill blocks server startup** — HTTP server can't bind until the async backfill yields, causing health checks to time out
- **No date-range filtering** — timestamps are stored but not exposed as search parameters, making "what happened last Tuesday?" queries impossible
- **No progress checkpointing** — if backfill crashes at transcript 5,000 of 10,000, all progress is lost

This fork fixes all of these and adds features for multi-project, date-aware session search.

## Changes from upstream

### Reliable backfill (SR-1)
1. **50ms throttle between inserts** — prevents Milvus-lite gRPC `GOAWAY` / `ENHANCE_YOUR_CALM` disconnections that silently killed the backfill loop
2. **Progress checkpoints every 100 files** — `index_state.json` is saved periodically so crashes don't lose all progress
3. **Milvus error recovery** — on connection errors, backfill pauses 5s for gRPC to recover instead of silently failing all subsequent inserts
4. **Startup delay** — backfill waits 3s after server init so HTTP can bind before embedding work starts, preventing health check timeouts

### Project-root resolution (SR-2)
5. **Auto-derive project root from slug** — slug names like `-Volumes-DATA-GitHub-Forge` are resolved to `/Volumes/DATA/GitHub/Forge` by greedy path reconstruction with filesystem verification. Newly derived mappings are cached in `slug_map.json`
6. **Transcript CWD fallback** — when slug resolution fails (e.g. slugs with dashes in directory names like `spec-workflow-mcp`), the first 30 lines of the transcript are peeked for a `cwd` field, which is resolved to its git root
7. **Skip bare `-` slug** — the generic unscoped transcript directory no longer maps to `/`, preventing bogus project_root entries

### Date-range filtering (SR-3)
8. **`date_from` / `date_to` parameters** on `search_all_sessions` — ISO 8601 date strings (e.g. `2026-04-08`) filter both Milvus vector search and FTS5 keyword search
9. **Inclusive date ranges** — `date_to` automatically appends `T23:59:59` for end-of-day inclusion

## Installation

Same as upstream — see [UPSTREAM-README.md](UPSTREAM-README.md) for full setup docs.

```bash
./setup.sh        # Sets up venv, deps, model, hooks, global MCP server
# Restart Claude Code to activate
```

## MCP Tools

All upstream tools are preserved. New parameters marked with **(fork)**.

| Tool | Description |
|------|-------------|
| `search_session` | Search current session with recency bias. |
| `search_all_sessions` | Cross-session semantic search. Optional `git_branch`, `date_from` **(fork)**, `date_to` **(fork)** filters. |
| `get_turns` | Retrieve turns around a specific turn index. |
| `get_session_stats` | Index statistics: turn count, session count, branches. |
| `cleanup_sessions` | Delete old session data by age, session ID, or branch. |

### Date-range query examples

```
search_all_sessions("deployment decisions", date_from="2026-04-08", date_to="2026-04-08")
search_all_sessions("what broke in production", date_from="2026-04-01", date_to="2026-04-07")
```

## Architecture

See [UPSTREAM-README.md](UPSTREAM-README.md) for the full architecture diagram and detailed component descriptions. This fork modifies:

- **`file_watcher.py`** — slug-to-path derivation, transcript CWD fallback, throttled backfill with checkpoints
- **`transcript_parser.py`** — `detect_project_root()` function for CWD extraction from transcript JSON
- **`rag_engine.py`** — date filter support in search queries
- **`fts_hybrid.py`** — date filter support in FTS5 queries
- **`tools.py`** — `date_from`/`date_to` parameters on `search_all_sessions`
- **`http_server.py`** — startup delay for backfill task

## Upstream

- **Repo**: [mwgreen/claude-code-session-rag](https://github.com/mwgreen/claude-code-session-rag)
- **Upstream README**: [UPSTREAM-README.md](UPSTREAM-README.md)
- **Fork point**: `637e6f4` (Global MCP server: replace per-project .mcp.json with user-scope headersHelper)
