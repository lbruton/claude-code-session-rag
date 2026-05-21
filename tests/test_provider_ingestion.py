"""SESF-10 regression tests for provider parse-to-index execution."""

import pytest


@pytest.mark.anyio
async def test_provider_ingestion_drains_codex_job_to_index_and_cursor(
    tmp_path,
    synthetic_codex_home,
    monkeypatch,
):
    import provider_ingestion
    import rag_engine
    import transcript_parser
    from backfill_manager import BackfillManager
    from provider_codex import CodexAdapter

    state_path = tmp_path / ".sessionflow" / "index_state.json"
    monkeypatch.setattr(transcript_parser, "_STATE_DIR", state_path.parent)
    monkeypatch.setattr(transcript_parser, "_STATE_PATH", state_path)

    indexed_batches = []

    async def fake_add_turns(turns, db_path=None):
        indexed_batches.append((turns, db_path))
        return len(turns)

    monkeypatch.setattr(rag_engine, "add_turns_async", fake_add_turns)

    manager = BackfillManager(tmp_path / ".sessionflow" / "backfill_state.json")
    manager.enqueue_provider_backfill(provider="codex", mode="recent")
    service = provider_ingestion.ProviderIngestionService(
        manager,
        db_path=str(tmp_path / "milvus.db"),
        adapters={"codex": CodexAdapter(home=synthetic_codex_home)},
    )

    result = await service.process_queued_jobs()

    assert result["jobs"] == 1
    assert result["processed_sources"] == 1
    assert result["indexed_turns"] == 1
    assert indexed_batches
    assert indexed_batches[0][0][0]["provider"] == "codex"
    assert "synthetic codex question" in indexed_batches[0][0][0]["text"]
    state = transcript_parser.load_index_state()
    assert state["providers"]["codex"]
    assert manager.status().jobs == []


def test_provider_ingestion_startup_uses_recent_not_full(tmp_path):
    from backfill_manager import BackfillManager

    manager = BackfillManager(tmp_path / "backfill_state.json")
    manager.enqueue_startup_defaults(
        enabled_providers=["claude_code_cli", "codex", "opencode"],
        mode="full",
    )

    assert {job.mode for job in manager.status().jobs} == {"recent"}

