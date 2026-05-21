"""SESF-6 red-phase tests for provider-aware backfill orchestration.

Requirements: 6, 7, 8.
"""


def test_backfill_manager_dedups_provider_jobs_and_tracks_progress(tmp_path):
    from backfill_manager import BackfillManager

    manager = BackfillManager(state_path=tmp_path / "backfill-state.json")

    first = manager.enqueue_provider_backfill(provider="codex", mode="recent", priority=10)
    second = manager.enqueue_provider_backfill(provider="codex", mode="recent", priority=10)

    assert first.job_id == second.job_id
    status = manager.status()
    assert status.providers["codex"].queued_jobs == 1
    assert status.providers["codex"].mode == "recent"


def test_backfill_manager_pause_resume_can_scope_to_one_provider(tmp_path):
    from backfill_manager import BackfillManager

    manager = BackfillManager(state_path=tmp_path / "backfill-state.json")
    manager.enqueue_provider_backfill(provider="codex", mode="full")
    manager.enqueue_provider_backfill(provider="opencode", mode="full")

    manager.pause(provider="codex")

    status = manager.status()
    assert status.providers["codex"].paused is True
    assert status.providers["opencode"].paused is False

    manager.resume(provider="codex")
    assert manager.status().providers["codex"].paused is False


def test_concurrent_enqueue_does_not_lose_jobs(tmp_path):
    """Several threads enqueueing distinct provider/mode pairs must all show up
    in the queue. Without the RLock the dict assignments + save_state() can
    interleave and clobber each other."""
    import threading

    from backfill_manager import BackfillManager

    manager = BackfillManager(state_path=tmp_path / "backfill-state.json")

    # Mix of providers/modes so each call produces a unique job_id.
    plan = [
        ("codex", "recent"),
        ("opencode", "recent"),
        ("antigravity_cli", "full"),
        ("antigravity_desktop", "incremental"),
        ("claude_code_cli", "full"),
        ("codex", "incremental"),
        ("opencode", "full"),
    ]

    def _worker(provider, mode):
        manager.enqueue_provider_backfill(provider=provider, mode=mode)

    threads = [threading.Thread(target=_worker, args=p) for p in plan]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    status = manager.status()
    assert len(status.jobs) == len(plan)


def test_global_pause_propagates_to_embedding_budget(tmp_path, monkeypatch):
    """Global BackfillManager.pause() must also stop the shared EmbeddingBudget,
    otherwise before_batch keeps approving work after the queue is paused."""
    import embedding_control
    from backfill_manager import BackfillManager

    monkeypatch.delenv("SESSIONFLOW_BACKFILL_PAUSED", raising=False)
    embedding_control.reset_embedding_budget()

    manager = BackfillManager(state_path=tmp_path / "backfill-state.json")
    budget = embedding_control.get_embedding_budget()
    assert budget.paused is False

    manager.pause()
    assert embedding_control.get_embedding_budget().paused is True

    manager.resume()
    assert embedding_control.get_embedding_budget().paused is False


def test_startup_plan_never_queues_all_provider_full_history_by_default(tmp_path):
    from backfill_manager import BackfillManager

    manager = BackfillManager(state_path=tmp_path / "backfill-state.json")
    manager.enqueue_startup_defaults(enabled_providers=["claude_code_cli", "codex", "opencode", "antigravity_cli"])

    queued_modes = {job.mode for job in manager.status().jobs}

    assert "full" not in queued_modes
    assert queued_modes <= {"incremental", "recent"}
