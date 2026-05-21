"""SESF-6 red-phase tests for local-only embedding resource controls.

Requirements: 6.6, 6.7, 7.
"""

import time


def test_embedding_budget_reads_local_resource_limits_from_env(monkeypatch):
    from embedding_control import EmbeddingBudget

    monkeypatch.setenv("SESSIONFLOW_EMBED_BATCH_SIZE", "8")
    monkeypatch.setenv("SESSIONFLOW_EMBED_COOLDOWN_MS", "250")
    monkeypatch.setenv("SESSIONFLOW_BACKFILL_MAX_TURNS_PER_RUN", "50")
    monkeypatch.setenv("SESSIONFLOW_BACKFILL_RECENT_DAYS", "14")
    monkeypatch.setenv("SESSIONFLOW_BACKFILL_MODE", "recent")

    budget = EmbeddingBudget.from_env()

    assert budget.batch_size == 8
    assert budget.cooldown_ms == 250
    assert budget.max_turns_per_run == 50
    assert budget.recent_days == 14
    assert budget.mode == "recent"


def test_embedding_budget_pause_blocks_backfill_batches(monkeypatch):
    from embedding_control import EmbeddingBudget

    monkeypatch.setenv("SESSIONFLOW_BACKFILL_PAUSED", "true")

    budget = EmbeddingBudget.from_env()

    assert budget.paused is True
    assert budget.before_batch(batch_size=1, estimated_chars=20).allowed is False
    assert "paused" in budget.before_batch(batch_size=1, estimated_chars=20).reason.lower()


def test_embedding_identity_is_local_mlx_and_ignores_openai_env(monkeypatch):
    from embedding_control import EmbeddingIdentity

    monkeypatch.setenv("OPENAI_API_KEY", "sk-synthetic-not-used")
    monkeypatch.setenv("SESSIONFLOW_MODEL", "embeddinggemma")

    identity = EmbeddingIdentity.current_local()

    assert identity.embedding_provider == "local_mlx"
    assert identity.model_name == "embeddinggemma"
    assert identity.dimension == 768
    assert not hasattr(identity, "api_key")


def test_embedding_budget_records_cooldown_after_batch():
    from embedding_control import EmbeddingBudget

    budget = EmbeddingBudget(batch_size=4, cooldown_ms=200, max_turns_per_run=10)
    budget.after_batch(duration=0.05, turns=4)
    decision = budget.before_batch(batch_size=4, estimated_chars=100)

    assert decision.allowed is False
    assert decision.retry_after_seconds > 0
    time.sleep(decision.retry_after_seconds)
    assert budget.before_batch(batch_size=4, estimated_chars=100).allowed is True
