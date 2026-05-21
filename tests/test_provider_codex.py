"""SESF-6 red-phase tests for Codex rollout JSONL ingestion.

Requirements: 2, 5, 6.
"""


def test_codex_adapter_dedups_active_and_archived_paths_by_logical_session(synthetic_codex_home):
    from provider_codex import CodexAdapter

    adapter = CodexAdapter(home=synthetic_codex_home)
    sources = adapter.discover_sources()

    logical_sources = [src for src in sources if src.logical_session_id == "synthetic-session"]
    assert len(logical_sources) == 1
    assert logical_sources[0].provider == "codex"
    assert logical_sources[0].source_kind == "codex_rollout_jsonl"

    parsed = adapter.parse_source(logical_sources[0], cursor=None)
    doc_ids = [turn["doc_id"] for turn in parsed.turns]
    assert parsed.turns
    assert len(doc_ids) == len(set(doc_ids))
    assert set(parsed.cursor["known_paths"]) >= {
        str(synthetic_codex_home / ".codex" / "sessions" / "2026" / "05" / "21" / "rollout-synthetic-session.jsonl"),
        str(synthetic_codex_home / ".codex" / "archived_sessions" / "rollout-synthetic-session.jsonl"),
    }


def test_codex_parse_falls_back_when_known_paths_cache_is_stale(synthetic_codex_home):
    """Sharing one CodexAdapter instance across discover/parse pairs (and
    between threads) means _known_paths can be wiped by a concurrent
    discover_sources(). parse_source() must not crash or silently drop the
    session — fall back to the source's own path."""
    from provider_codex import CodexAdapter

    adapter = CodexAdapter(home=synthetic_codex_home)
    source = next(s for s in adapter.discover_sources() if s.logical_session_id == "synthetic-session")

    # Simulate a second discover pass clearing the cache mid-flight.
    adapter._known_paths = {}

    parsed = adapter.parse_source(source, cursor=None)
    assert parsed.turns, "fallback should still parse the source file"
    assert parsed.cursor["known_paths"], "cursor still records at least the fallback path"


def test_codex_adapter_marks_unknown_project_scope_when_cwd_missing(tmp_path):
    from provider_codex import CodexAdapter

    home = tmp_path / "home"
    root = home / ".codex" / "sessions" / "2026" / "05" / "21"
    root.mkdir(parents=True)
    (root / "rollout-no-cwd.jsonl").write_text('{"session_id":"no-cwd","type":"response_item","content":"synthetic"}\n')

    source = CodexAdapter(home=home).discover_sources()[0]

    assert source.project_root in {"", "unknown", "/"}
    assert source.status == "eligible"
