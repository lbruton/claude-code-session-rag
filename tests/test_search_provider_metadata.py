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
    # SESF-16: query is now optional (empty query → recency listing), so the
    # schema requires no fields.
    assert schema["required"] == []


def test_search_rejects_invalid_provider_before_hitting_milvus(monkeypatch):
    """provider/source_kind flow into a raw Milvus filter expression — reject
    junk inputs at the entry point so untrusted callers can't inject filter
    fragments. The validation must fire before any Milvus client connects."""
    import importlib

    rag_engine = importlib.import_module("rag_engine")

    def _explode(*_a, **_kw):
        raise AssertionError("milvus_client should not be reached for invalid input")

    monkeypatch.setattr(rag_engine, "milvus_client", _explode)
    monkeypatch.setattr(rag_engine, "embed_texts", lambda *a, **kw: [[0.0] * 768])

    import pytest

    with pytest.raises(ValueError, match="Invalid provider"):
        rag_engine.search("hello", provider='codex"; DROP TABLE sessions; --')

    with pytest.raises(ValueError, match="Invalid source_kind"):
        rag_engine.search("hello", source_kind="not-a-real-kind")
