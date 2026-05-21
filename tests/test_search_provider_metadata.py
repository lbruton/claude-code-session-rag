"""SESF-6 red-phase tests for provider-aware search output and filters.

Requirements: 5, 7.6, 8.
"""

import importlib


def test_format_results_includes_provider_and_source_labels(stub_rag_engine):
    tools = importlib.import_module("tools")

    rendered = tools.format_results([{
        "content": "synthetic codex result",
        "session_id": "synthetic-session",
        "logical_session_id": "synthetic-session",
        "provider": "codex",
        "source_kind": "codex_rollout_jsonl",
        "source_class": "native",
        "project_root": "/tmp/project",
        "turn_index": 4,
        "chunk_type": "turn",
        "distance": 0.1,
    }])

    assert "provider:codex" in rendered
    assert "source:codex_rollout_jsonl" in rendered


def test_format_stats_includes_provider_counts(stub_rag_engine):
    tools = importlib.import_module("tools")

    rendered = tools.format_stats({
        "total_turns": 3,
        "sessions": 2,
        "by_type": {"turn": 3},
        "providers": {"claude_code_cli": 1, "codex": 2},
    }, db_path="/tmp/milvus.db")

    assert "claude_code_cli" in rendered
    assert "codex" in rendered


def test_mcp_search_schema_exposes_optional_provider_filters_without_requiring_them(stub_rag_engine):
    from tools import build_search_all_sessions_schema

    schema = build_search_all_sessions_schema()

    assert "provider" in schema["properties"]
    assert "source_kind" in schema["properties"]
    assert schema["required"] == ["query"]
