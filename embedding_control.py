"""Local-only embedding resource controls for SessionFlow backfill work."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import os
import time
from typing import Optional


LOCAL_MODEL_DIMS = {
    "embeddinggemma": 768,
    "modernbert": 768,
}


def _env_int(name: str, default: int, minimum: Optional[int] = None) -> int:
    try:
        value = int(os.getenv(name, str(default)))
    except ValueError:
        value = default
    if minimum is not None:
        value = max(value, minimum)
    return value


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.lower() in {"1", "true", "yes", "on"}


@dataclass
class EmbeddingDecision:
    allowed: bool
    reason: str = ""
    retry_after_seconds: float = 0.0


@dataclass
class EmbeddingIdentity:
    embedding_provider: str
    model_name: str
    dimension: int
    collection_name: str = "sessions"
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    @classmethod
    def current_local(cls) -> "EmbeddingIdentity":
        model_name = os.getenv("SESSIONFLOW_MODEL", "embeddinggemma").lower()
        dimension = LOCAL_MODEL_DIMS.get(model_name)
        if dimension is None:
            raise ValueError(
                f"Unknown local embedding model {model_name!r}; "
                f"valid options: {', '.join(sorted(LOCAL_MODEL_DIMS))}"
            )
        return cls(
            embedding_provider="local_mlx",
            model_name=model_name,
            dimension=dimension,
        )


@dataclass
class EmbeddingBudget:
    batch_size: int = 16
    cooldown_ms: int = 200
    max_turns_per_run: int = 200
    max_files_per_run: int = 100
    recent_days: int = 14
    mode: str = "recent"
    paused: bool = False
    turns_processed: int = 0
    batches_processed: int = 0
    errors: int = 0
    last_batch_finished_at: float = 0.0
    last_batch_duration: float = 0.0

    @classmethod
    def from_env(cls) -> "EmbeddingBudget":
        mode = os.getenv("SESSIONFLOW_BACKFILL_MODE", "recent").lower()
        if mode not in {"recent", "incremental", "full"}:
            mode = "recent"
        return cls(
            batch_size=_env_int("SESSIONFLOW_EMBED_BATCH_SIZE", 16, minimum=1),
            cooldown_ms=_env_int("SESSIONFLOW_EMBED_COOLDOWN_MS", 200, minimum=200),
            max_turns_per_run=_env_int("SESSIONFLOW_BACKFILL_MAX_TURNS_PER_RUN", 200, minimum=1),
            max_files_per_run=_env_int("SESSIONFLOW_BACKFILL_MAX_FILES_PER_RUN", 100, minimum=1),
            recent_days=_env_int("SESSIONFLOW_BACKFILL_RECENT_DAYS", 14, minimum=1),
            mode=mode,
            paused=_env_bool("SESSIONFLOW_BACKFILL_PAUSED", False),
        )

    def before_batch(self, batch_size: int, estimated_chars: int = 0) -> EmbeddingDecision:
        if self.paused:
            return EmbeddingDecision(False, "Backfill embedding is paused")
        if self.turns_processed >= self.max_turns_per_run:
            return EmbeddingDecision(False, "Backfill max turns per run reached")
        if batch_size > self.batch_size:
            return EmbeddingDecision(
                False,
                f"Batch size {batch_size} exceeds configured limit {self.batch_size}",
            )

        elapsed = time.monotonic() - self.last_batch_finished_at
        cooldown = self.cooldown_ms / 1000.0
        if self.last_batch_finished_at and elapsed < cooldown:
            return EmbeddingDecision(
                False,
                "Embedding cooldown active",
                retry_after_seconds=max(0.0, cooldown - elapsed),
            )

        return EmbeddingDecision(True)

    def after_batch(self, duration: float, turns: int, error: Optional[BaseException] = None) -> None:
        self.last_batch_finished_at = time.monotonic()
        self.last_batch_duration = duration
        self.batches_processed += 1
        self.turns_processed += turns
        if error is not None:
            self.errors += 1

    def split_batches(self, turns: list) -> list[list]:
        return [turns[i:i + self.batch_size] for i in range(0, len(turns), self.batch_size)]

    def status(self) -> dict:
        return {
            "embedding_provider": "local_mlx",
            "model_name": EmbeddingIdentity.current_local().model_name,
            "batch_size": self.batch_size,
            "cooldown_ms": self.cooldown_ms,
            "max_turns_per_run": self.max_turns_per_run,
            "max_files_per_run": self.max_files_per_run,
            "recent_days": self.recent_days,
            "mode": self.mode,
            "paused": self.paused,
            "turns_processed": self.turns_processed,
            "batches_processed": self.batches_processed,
            "errors": self.errors,
        }


_BUDGET: Optional[EmbeddingBudget] = None


def get_embedding_budget() -> EmbeddingBudget:
    global _BUDGET
    if _BUDGET is None:
        _BUDGET = EmbeddingBudget.from_env()
    return _BUDGET


def reset_embedding_budget() -> EmbeddingBudget:
    global _BUDGET
    _BUDGET = EmbeddingBudget.from_env()
    return _BUDGET
