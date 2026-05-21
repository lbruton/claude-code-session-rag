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


def test_codex_adapter_marks_unknown_project_scope_when_cwd_missing(tmp_path):
    from provider_codex import CodexAdapter

    home = tmp_path / "home"
    root = home / ".codex" / "sessions" / "2026" / "05" / "21"
    root.mkdir(parents=True)
    (root / "rollout-no-cwd.jsonl").write_text('{"session_id":"no-cwd","type":"response_item","content":"synthetic"}\n')

    source = CodexAdapter(home=home).discover_sources()[0]

    assert source.project_root in {"", "unknown", "/"}
    assert source.status == "eligible"
