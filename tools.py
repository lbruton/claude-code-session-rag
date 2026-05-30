"""
MCP tool definitions and project context for SessionFlow.
"""

import asyncio
import contextvars
import os
from pathlib import Path
from mcp.server import Server
from mcp import types

import rag_engine
from provider_adapters import LEGAL_PROVIDERS, LEGAL_SORT_BY, LEGAL_SOURCE_KINDS


# --- Project context ---

_current_project_root: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "current_project_root", default=None
)


def set_current_project_root(root: str | None):
    _current_project_root.set(root)


def get_current_project_root() -> str | None:
    return _current_project_root.get()


def get_db_path() -> str:
    """Milvus URI — remote Standalone if SESSIONFLOW_MILVUS_URI is set, else local Lite."""
    return os.getenv("SESSIONFLOW_MILVUS_URI", str(Path.home() / ".sessionflow" / "milvus.db"))


def _validate_enum_arg(name: str, value, legal_values) -> "types.TextContent | None":
    """Return an error TextContent if `value` is non-None and outside `legal_values`.

    Shared by the search handlers so provider/source_kind/sort_by validation stays
    consistent in one place rather than duplicated per tool.
    """
    if value is not None and value not in legal_values:
        allowed = ", ".join(sorted(legal_values))
        return types.TextContent(
            type="text",
            text=f"Invalid {name}: {value!r}; expected one of: {allowed}",
        )
    return None


# --- Formatting helpers ---

def format_results(results: list[dict]) -> str:
    """Format search results as markdown."""
    if not results:
        return "No results found."

    output = []
    for i, r in enumerate(results, 1):
        # Header with metadata
        session_id = r.get("session_id", "")
        branch = r.get("git_branch", "")
        ts = r.get("timestamp", "")[:19]  # trim to readable
        chunk_type = r.get("chunk_type", "turn")
        similarity = 1 - r.get("distance", 0)

        turn_index = r.get("turn_index", 0)

        project = r.get("project_root", "")
        provider = r.get("provider", "")
        source_kind = r.get("source_kind", "")

        header_parts = [f"**Result {i}**"]
        if ts:
            header_parts.append(f"({ts})")
        if branch:
            header_parts.append(f"[{branch}]")
        if project:
            header_parts.append(f"project:{Path(project).name}")
        if provider:
            header_parts.append(f"provider:{provider}")
        if source_kind:
            header_parts.append(f"source:{source_kind}")
        if session_id:
            header_parts.append(f"session:{session_id}")

        output.append(" ".join(header_parts))

        meta = f"*Turn: {turn_index} | Type: {chunk_type} | Relevance: {similarity:.2f}*"
        output.append(meta)
        output.append("")
        output.append(r.get("content", ""))
        output.append("")
        output.append("---")
        output.append("")

    return "\n".join(output)


def format_turns(results: list[dict]) -> str:
    """Format get_turns results as markdown."""
    if not results:
        return "No turns found."

    output = []
    for r in results:
        turn_index = r.get("turn_index", 0)
        ts = r.get("timestamp", "")[:19]
        chunk_type = r.get("chunk_type", "turn")
        branch = r.get("git_branch", "")

        header_parts = [f"**Turn {turn_index}**"]
        if ts:
            header_parts.append(f"({ts})")
        if branch:
            header_parts.append(f"[{branch}]")

        output.append(" ".join(header_parts))
        output.append(f"*Type: {chunk_type}*")
        output.append("")
        output.append(r.get("content", ""))
        output.append("")
        output.append("---")
        output.append("")

    return "\n".join(output)


def format_stats(stats: dict, db_path: str) -> str:
    """Format index statistics."""
    lines = [
        f"**Total Turns:** {stats['total_turns']}",
        f"**Sessions:** {stats['sessions']}",
    ]

    if stats.get("branches"):
        lines.append(f"**Branches:** {', '.join(stats['branches'])}")

    if stats.get("by_type"):
        lines.append("\n### By Type")
        for t, count in sorted(stats["by_type"].items(), key=lambda x: x[1], reverse=True):
            lines.append(f"- {t}: {count}")

    if stats.get("providers"):
        lines.append("\n### Providers")
        for provider, count in sorted(stats["providers"].items(), key=lambda x: x[1], reverse=True):
            lines.append(f"- {provider}: {count}")

    lines.append(f"\n**Index Location:** {db_path}")
    return "\n".join(lines)


# --- Tool registration ---


def build_search_all_sessions_schema() -> dict:
    return {
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": "Natural language search query. Omit (or pass empty) to list the most recent turns chronologically, newest first.",
            },
            "n": {
                "type": "integer",
                "description": "Number of results to return (default: 10)",
                "default": 10,
            },
            "git_branch": {
                "type": "string",
                "description": "Filter by git branch name (e.g., 'develop', 'feature/my-feature')",
            },
            "project_root": {
                "type": "string",
                "description": "Filter to a specific project path, or '*' for all projects. Default: current project.",
            },
            "provider": {
                "type": "string",
                "description": "Optional provider filter (e.g., codex, opencode, antigravity_cli)",
            },
            "source_kind": {
                "type": "string",
                "description": "Optional provider source-kind filter (e.g., codex_rollout_jsonl)",
            },
            "sort_by": {
                "type": "string",
                "enum": sorted(LEGAL_SORT_BY),
                "description": "Ranking strategy: 'relevance' (pure RRF relevance), 'recency' (newest first), or 'hybrid' (blended, default).",
                "default": "hybrid",
            },
            "date_from": {
                "type": "string",
                "description": "ISO date lower bound, inclusive (e.g., '2026-04-02'). Only returns turns on or after this date.",
            },
            "date_to": {
                "type": "string",
                "description": "ISO date upper bound, inclusive (e.g., '2026-04-02'). Only returns turns on or before this date.",
            },
        },
        "required": [],
    }

def register_tools(server: Server):
    """Register SessionFlow MCP tools."""

    @server.list_tools()
    async def list_tools() -> list[types.Tool]:
        return [
            types.Tool(
                name="search_session",
                description=(
                    "Search conversation history for past discussions, decisions, "
                    "code snippets, and error messages. Ranked by 'hybrid' (blended "
                    "semantic relevance + recency) by default; pass sort_by to choose "
                    "'relevance' or 'recency'."
                ),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "Natural language search query (e.g., 'approval workflow decision', 'error in deploy script'). Omit (or pass empty) to list the most recent turns chronologically, newest first.",
                        },
                        "n": {
                            "type": "integer",
                            "description": "Number of results to return (default: 5)",
                            "default": 5,
                        },
                        "session_id": {
                            "type": "string",
                            "description": "Claude session ID to filter to. Usually auto-resolved; only pass if auto-resolution fails.",
                        },
                        "sort_by": {
                            "type": "string",
                            "enum": ["relevance", "recency", "hybrid"],
                            "description": "Ranking strategy: 'relevance' (pure semantic), 'recency' (newest first), or 'hybrid' (blended, default).",
                            "default": "hybrid",
                        },
                    },
                    "required": [],
                },
            ),
            types.Tool(
                name="search_all_sessions",
                description=(
                    "Search across ALL past conversation sessions. Ranked by 'hybrid' "
                    "(blended semantic relevance + recency) by default; pass sort_by to "
                    "choose 'relevance' or 'recency'. Optionally filter by git branch or "
                    "date range."
                ),
                inputSchema=build_search_all_sessions_schema(),
            ),
            types.Tool(
                name="get_turns",
                description=(
                    "Retrieve conversation turns surrounding a specific turn index within a session. "
                    "Use this after search_session or search_all_sessions to see the full context "
                    "around a search hit."
                ),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "session_id": {
                            "type": "string",
                            "description": "The session ID (from a search result)",
                        },
                        "turn_index": {
                            "type": "integer",
                            "description": "The turn index to center on (from a search result)",
                        },
                        "context": {
                            "type": "integer",
                            "description": "Number of turns before and after to include (default: 2)",
                            "default": 2,
                        },
                    },
                    "required": ["session_id", "turn_index"],
                },
            ),
            types.Tool(
                name="get_session_stats",
                description="Get session index statistics (turn count, session count, branches)",
                inputSchema={
                    "type": "object",
                    "properties": {},
                    "required": [],
                },
            ),
            types.Tool(
                name="cleanup_sessions",
                description=(
                    "Delete old session data from the index. "
                    "Can delete by age (days), specific session ID, or git branch."
                ),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "max_age_days": {
                            "type": "integer",
                            "description": "Delete turns older than this many days",
                        },
                        "session_id": {
                            "type": "string",
                            "description": "Delete all turns for this session ID",
                        },
                        "git_branch": {
                            "type": "string",
                            "description": "Delete all turns for this git branch",
                        },
                        "project_root": {
                            "type": "string",
                            "description": "Filter cleanup to a specific project path. Default: current project.",
                        },
                    },
                    "required": [],
                },
            ),
        ]

    @server.call_tool()
    async def call_tool(name: str, arguments: dict) -> list[types.TextContent]:
        db = get_db_path()
        current_project = get_current_project_root()

        try:
            if name == "search_session":
                session_id = arguments.get("session_id")
                sort_by_arg = arguments.get("sort_by", "hybrid")
                err = _validate_enum_arg("sort_by", sort_by_arg, LEGAL_SORT_BY)
                if err:
                    return [err]
                results = rag_engine.search(
                    arguments.get("query") or "",
                    arguments.get("n", 5),
                    session_id=session_id,
                    project_root=current_project,
                    sort_by=sort_by_arg,
                    db_path=db,
                )
                return [types.TextContent(type="text", text=format_results(results))]

            elif name == "search_all_sessions":
                # project_root scoping: default=current project, "*"=all projects
                pr_arg = arguments.get("project_root")
                if pr_arg == "*":
                    pr = None  # cross-project search
                elif pr_arg:
                    pr = pr_arg  # explicit project
                else:
                    pr = current_project  # default: current project

                provider_arg = arguments.get("provider")
                source_kind_arg = arguments.get("source_kind")
                sort_by_arg = arguments.get("sort_by", "hybrid")
                for err in (
                    _validate_enum_arg("provider", provider_arg, LEGAL_PROVIDERS),
                    _validate_enum_arg("source_kind", source_kind_arg, LEGAL_SOURCE_KINDS),
                    _validate_enum_arg("sort_by", sort_by_arg, LEGAL_SORT_BY),
                ):
                    if err:
                        return [err]

                results = rag_engine.search(
                    arguments.get("query") or "",
                    arguments.get("n", 10),
                    git_branch=arguments.get("git_branch"),
                    project_root=pr,
                    sort_by=sort_by_arg,
                    date_from=arguments.get("date_from"),
                    date_to=arguments.get("date_to"),
                    provider=arguments.get("provider"),
                    source_kind=arguments.get("source_kind"),
                    db_path=db,
                )
                return [types.TextContent(type="text", text=format_results(results))]

            elif name == "get_turns":
                results = rag_engine.get_turns(
                    arguments["session_id"],
                    arguments["turn_index"],
                    context=arguments.get("context", 2),
                    db_path=db,
                )
                return [types.TextContent(type="text", text=format_turns(results))]

            elif name == "get_session_stats":
                stats = rag_engine.get_stats(
                    project_root=current_project,
                    db_path=db,
                )
                return [types.TextContent(type="text", text=format_stats(stats, db))]

            elif name == "cleanup_sessions":
                max_age = arguments.get("max_age_days")
                sid = arguments.get("session_id")
                branch = arguments.get("git_branch")

                if not any([max_age, sid, branch]):
                    return [types.TextContent(
                        type="text",
                        text="Specify at least one of: max_age_days, session_id, git_branch",
                    )]

                parts = []
                if max_age:
                    count = rag_engine.delete_older_than(max_age, db_path=db)
                    parts.append(f"Deleted {count} turns older than {max_age} days")
                if sid:
                    count = rag_engine.delete_by_session(sid, db_path=db)
                    parts.append(f"Deleted {count} turns for session {sid[:12]}")
                if branch:
                    count = rag_engine.delete_by_branch(branch, db_path=db)
                    parts.append(f"Deleted {count} turns for branch '{branch}'")

                stats = rag_engine.get_stats(
                    project_root=current_project,
                    db_path=db,
                )
                parts.append(f"\nRemaining: {stats['total_turns']} turns across {stats['sessions']} sessions")
                return [types.TextContent(type="text", text="\n".join(parts))]

            else:
                raise ValueError(f"Unknown tool: {name}")

        except (Exception, asyncio.CancelledError) as e:
            return [types.TextContent(type="text", text=f"Error executing {name}: {str(e)}")]
