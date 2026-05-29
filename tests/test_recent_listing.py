"""SESF-16 red-phase tests for query-optional chronological recency listing.

When ``query`` is absent/empty, ``search_all_sessions`` / ``search_session``
must fall back to listing recent turns sorted purely by timestamp — without
embedding the (empty) query or running an FTS MATCH. The engine reaches Milvus
via a filter-only ``client.query()`` (Option A: capped scan + client-side
timestamp sort, mirroring ``get_turns``/``get_stats``).

System under test = ``rag_engine.search`` and the two MCP tools in ``tools``.
"""

from __future__ import annotations

import asyncio
import importlib
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

import pytest

rag_engine = importlib.import_module("rag_engine")
tools = importlib.import_module("tools")


NOW = datetime(2026, 5, 28, 12, 0, 0, tzinfo=timezone.utc)
DB = "/tmp/sessionflow-test.db"


def _iso(days_ago: float) -> str:
    return (NOW - timedelta(days=days_ago)).isoformat()


def _qrow(doc_id: str, days_ago: float | None = None, **extra) -> dict:
    """A Milvus ``query()`` row (flat entity dict, as the listing path reads)."""
    row = {
        "document": f"content {doc_id}",
        "doc_id": doc_id,
        "session_id": "s",
        "transcript_file": "",
        "turn_index": 0,
        "timestamp": _iso(days_ago) if days_ago is not None else "",
        "git_branch": "",
        "chunk_type": "turn",
        "project_root": "",
        "logical_session_id": "s",
        "provider": "claude_code_cli",
        "source_kind": "claude_code_jsonl",
        "source_class": "native",
        "source_id": "",
        "source_path": "",
    }
    row.update(extra)
    return row


class _ListingClient:
    """Fake Milvus client: serves query(), forbids vector search()."""

    def __init__(self, rows):
        self._rows = rows
        self.query_calls: list[dict] = []

    def query(self, *args, **kwargs):
        self.query_calls.append(kwargs)
        return [dict(r) for r in self._rows]

    def search(self, *args, **kwargs):
        raise AssertionError("vector search() must not run on the query-less listing path")


def _patch_listing(monkeypatch, rows) -> _ListingClient:
    """Patch embed/milvus/fts so the listing path runs without live services.

    embed_texts and the FTS MATCH both explode — the listing path must touch
    neither.
    """
    def _embed_explode(*a, **kw):
        raise AssertionError("embed_texts must not be called for an empty query")

    monkeypatch.setattr(rag_engine, "embed_texts", _embed_explode)

    client = _ListingClient(rows)

    @contextmanager
    def _fake_client(db_path=None):
        yield client

    monkeypatch.setattr(rag_engine, "milvus_client", _fake_client)

    def _fts_explode(*a, **kw):
        raise AssertionError("FTS MATCH must not run for an empty query")

    monkeypatch.setattr(rag_engine._fts, "search", _fts_explode)
    return client


# ---------------------------------------------------------------------------
# Engine: empty/whitespace/None query → recency listing
# ---------------------------------------------------------------------------

def test_empty_query_lists_recent_without_embed_or_fts(monkeypatch):
    rows = [_qrow("old", 10), _qrow("new", 1), _qrow("mid", 5)]
    _patch_listing(monkeypatch, rows)
    out = rag_engine.search("", n=5, db_path=DB)
    assert [r["doc_id"] for r in out] == ["new", "mid", "old"]


def test_whitespace_query_treated_as_empty(monkeypatch):
    rows = [_qrow("old", 10), _qrow("new", 1)]
    _patch_listing(monkeypatch, rows)
    out = rag_engine.search("   ", n=5, db_path=DB)
    assert [r["doc_id"] for r in out] == ["new", "old"]


def test_none_query_treated_as_empty(monkeypatch):
    rows = [_qrow("old", 10), _qrow("new", 1)]
    _patch_listing(monkeypatch, rows)
    out = rag_engine.search(None, n=5, db_path=DB)
    assert [r["doc_id"] for r in out] == ["new", "old"]


def test_listing_truncates_to_n(monkeypatch):
    rows = [_qrow(str(i), days_ago=i) for i in range(10)]
    _patch_listing(monkeypatch, rows)
    out = rag_engine.search("", n=3, db_path=DB)
    assert [r["doc_id"] for r in out] == ["0", "1", "2"]  # smallest days_ago = newest


def test_listing_missing_timestamp_sorts_last(monkeypatch):
    rows = [_qrow("nots", None), _qrow("new", 1), _qrow("old", 9)]
    _patch_listing(monkeypatch, rows)
    out = rag_engine.search("", n=5, db_path=DB)
    assert [r["doc_id"] for r in out] == ["new", "old", "nots"]


def test_listing_maps_document_to_content(monkeypatch):
    _patch_listing(monkeypatch, [_qrow("a", 1)])
    out = rag_engine.search("", n=5, db_path=DB)
    assert out[0]["content"] == "content a"
    assert "document" not in out[0]


def test_listing_applies_project_filter_to_query(monkeypatch):
    client = _patch_listing(monkeypatch, [_qrow("a", 1)])
    rag_engine.search("", n=5, project_root="/proj", db_path=DB)
    filter_expr = client.query_calls[0]["filter"]
    assert 'project_root == "/proj"' in filter_expr


def test_listing_applies_provider_and_date_filters(monkeypatch):
    client = _patch_listing(monkeypatch, [_qrow("a", 1)])
    rag_engine.search(
        "", n=5, provider="codex", date_from="2026-05-01", db_path=DB
    )
    filter_expr = client.query_calls[0]["filter"]
    assert 'provider == "codex"' in filter_expr
    assert 'timestamp >= "2026-05-01"' in filter_expr


def test_listing_uses_scan_cap_limit(monkeypatch):
    client = _patch_listing(monkeypatch, [_qrow("a", 1)])
    rag_engine.search("", n=5, db_path=DB)
    assert client.query_calls[0]["limit"] == rag_engine.RECENT_LISTING_SCAN_CAP


def test_listing_validates_provider_before_query(monkeypatch):
    # Invalid provider must still raise before any Milvus work, even query-less.
    def _explode(*a, **kw):
        raise AssertionError("milvus_client must not be reached for invalid provider")

    monkeypatch.setattr(rag_engine, "milvus_client", _explode)
    with pytest.raises(ValueError, match="provider"):
        rag_engine.search("", provider="bogus", db_path=DB)


# ---------------------------------------------------------------------------
# Engine: a real query still uses the full search pipeline
# ---------------------------------------------------------------------------

def test_nonempty_query_uses_search_pipeline(monkeypatch):
    called = {"embed": False}

    def _embed(*a, **kw):
        called["embed"] = True
        return [[0.0] * 768]

    monkeypatch.setattr(rag_engine, "embed_texts", _embed)

    class _C:
        def search(self, *a, **kw):
            return [[]]

        def query(self, *a, **kw):
            raise AssertionError("query path must not run for a real query")

    @contextmanager
    def _fake_client(db_path=None):
        yield _C()

    monkeypatch.setattr(rag_engine, "milvus_client", _fake_client)
    monkeypatch.setattr(rag_engine._fts, "search", lambda *a, **kw: [])

    import fts_hybrid
    monkeypatch.setattr(fts_hybrid, "fts_backfill_required", lambda *a, **kw: False)

    rag_engine.search("real query", db_path=DB)
    assert called["embed"] is True


# ---------------------------------------------------------------------------
# Tool layer: query becomes optional
# ---------------------------------------------------------------------------

def _tool_schemas():
    """Register tools against a capturing fake server; return {name: schema}."""
    captured: dict = {}

    class _Server:
        def list_tools(self):
            def deco(fn):
                captured["list"] = fn
                return fn
            return deco

        def call_tool(self):
            def deco(fn):
                captured["call"] = fn
                return fn
            return deco

    tools.register_tools(_Server())
    listed = asyncio.run(captured["list"]())
    return {t.name: t.inputSchema for t in listed}, captured["call"]


def test_search_all_sessions_schema_query_optional():
    schema = tools.build_search_all_sessions_schema()
    assert "query" not in schema.get("required", [])
    assert "query" in schema["properties"]  # still offered, just not required


def test_search_session_schema_query_optional():
    schemas, _ = _tool_schemas()
    assert "query" not in schemas["search_session"].get("required", [])


@pytest.mark.parametrize("tool_name", ["search_session", "search_all_sessions"])
def test_tool_allows_missing_query(monkeypatch, tool_name):
    seen: dict = {}

    def _spy(query, n=5, **kwargs):
        seen["query"] = query
        return []

    monkeypatch.setattr(tools.rag_engine, "search", _spy)
    monkeypatch.setattr(tools, "get_current_project_root", lambda: None)

    _, call_tool = _tool_schemas()
    result = asyncio.run(call_tool(tool_name, {}))  # no query supplied

    # Engine was invoked with an empty (not missing/KeyError) query, and the
    # handler returned a normal text payload rather than an error.
    assert seen.get("query") == ""
    assert result and result[0].type == "text"
    assert not result[0].text.startswith("Invalid")
