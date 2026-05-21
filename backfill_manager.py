"""Provider-aware backfill queue and progress state."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional
import hashlib
import json
import os
import threading
import time


@dataclass
class BackfillJob:
    job_id: str
    provider: str
    mode: str
    priority: int = 0
    source_id: str = ""
    since: str = ""
    limit: Optional[int] = None
    reason: str = ""
    created_at: float = field(default_factory=time.time)


@dataclass
class ProviderBackfillStatus:
    provider: str
    mode: str = ""
    queued_jobs: int = 0
    paused: bool = False
    processed_sources: int = 0
    indexed_turns: int = 0
    error_count: int = 0


@dataclass
class BackfillStatus:
    jobs: List[BackfillJob]
    providers: Dict[str, ProviderBackfillStatus]
    paused: bool = False


class BackfillManager:
    """Small durable queue for provider-scoped backfill work."""

    def __init__(self, state_path: str | Path):
        self.state_path = Path(state_path)
        self.jobs: Dict[str, BackfillJob] = {}
        self.paused_providers: set[str] = set()
        self.global_paused = False
        self.provider_stats: Dict[str, ProviderBackfillStatus] = {}
        # RLock so internal methods that already hold the lock (e.g.
        # enqueue_provider_backfill -> save_state) don't deadlock.
        self._lock = threading.RLock()
        self.load_state()

    def _job_key(
        self,
        provider: str,
        mode: str,
        source_id: str = "",
        since: str = "",
        limit: Optional[int] = None,
    ) -> str:
        raw = json.dumps({
            "provider": provider,
            "mode": mode,
            "source_id": source_id,
            "since": since,
            "limit": limit,
        }, sort_keys=True)
        digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]
        return f"{provider}:{mode}:{digest}"

    def _ensure_provider(self, provider: str) -> ProviderBackfillStatus:
        if provider not in self.provider_stats:
            self.provider_stats[provider] = ProviderBackfillStatus(provider=provider)
        return self.provider_stats[provider]

    def enqueue_provider_backfill(
        self,
        provider: str,
        mode: str,
        limit: Optional[int] = None,
        since: str = "",
        priority: int = 0,
    ) -> BackfillJob:
        if mode not in {"recent", "incremental", "full"}:
            raise ValueError(f"Unsupported backfill mode: {mode}")
        with self._lock:
            job_id = self._job_key(provider, mode, since=since, limit=limit)
            if job_id not in self.jobs:
                self.jobs[job_id] = BackfillJob(
                    job_id=job_id,
                    provider=provider,
                    mode=mode,
                    priority=priority,
                    since=since,
                    limit=limit,
                    reason=f"{mode} provider backfill",
                )
                self.save_state()
            provider_status = self._ensure_provider(provider)
            provider_status.mode = mode
            provider_status.queued_jobs = sum(1 for job in self.jobs.values() if job.provider == provider)
            return self.jobs[job_id]

    def enqueue_source(self, source_id: str, provider: str, reason: str = "source changed") -> BackfillJob:
        with self._lock:
            job_id = self._job_key(provider, "manual-source", source_id=source_id)
            if job_id not in self.jobs:
                self.jobs[job_id] = BackfillJob(
                    job_id=job_id,
                    provider=provider,
                    mode="manual-source",
                    source_id=source_id,
                    reason=reason,
                )
                self.save_state()
            self._ensure_provider(provider).queued_jobs = sum(
                1 for job in self.jobs.values() if job.provider == provider
            )
            return self.jobs[job_id]

    def enqueue_startup_defaults(self, enabled_providers: List[str], mode: str = "recent") -> List[BackfillJob]:
        startup_mode = mode if mode in {"recent", "incremental"} else "recent"
        with self._lock:
            return [
                self.enqueue_provider_backfill(provider=provider, mode=startup_mode, priority=1)
                for provider in enabled_providers
            ]

    def pause(self, provider: Optional[str] = None) -> None:
        with self._lock:
            if provider:
                self.paused_providers.add(provider)
                self._ensure_provider(provider).paused = True
            else:
                self.global_paused = True
                # Global pause must also stop the shared embedding budget
                # — per-provider pause is queue-only and does NOT halt embeds
                # already in flight from other providers.
                self._propagate_budget_pause(True)
            self.save_state()

    def resume(self, provider: Optional[str] = None) -> None:
        with self._lock:
            if provider:
                self.paused_providers.discard(provider)
                self._ensure_provider(provider).paused = False
            else:
                self.global_paused = False
                self._propagate_budget_pause(False)
            self.save_state()

    @staticmethod
    def _propagate_budget_pause(paused: bool) -> None:
        """Mirror global pause/resume into the shared EmbeddingBudget.

        Imported lazily to avoid pulling embedding_control into modules that
        only need queue accounting (e.g. tests that monkeypatch rag_engine).
        """
        try:
            from embedding_control import get_embedding_budget
        except Exception:
            return
        try:
            get_embedding_budget().paused = paused
        except Exception:
            return

    def status(self) -> BackfillStatus:
        with self._lock:
            for provider in {job.provider for job in self.jobs.values()} | set(self.provider_stats):
                provider_status = self._ensure_provider(provider)
                provider_status.queued_jobs = sum(
                    1 for job in self.jobs.values() if job.provider == provider
                )
                provider_status.paused = provider in self.paused_providers
            ordered_jobs = sorted(self.jobs.values(), key=lambda job: (-job.priority, job.created_at))
            return BackfillStatus(
                jobs=ordered_jobs,
                providers=dict(self.provider_stats),
                paused=self.global_paused,
            )

    def load_state(self) -> None:
        with self._lock:
            if not self.state_path.exists():
                return
            try:
                data = json.loads(self.state_path.read_text())
            except (OSError, json.JSONDecodeError):
                return
            self.global_paused = bool(data.get("global_paused", False))
            self.paused_providers = set(data.get("paused_providers", []))
            self.jobs = {}
            for item in data.get("jobs", []):
                if not isinstance(item, dict) or "job_id" not in item:
                    continue
                try:
                    self.jobs[item["job_id"]] = BackfillJob(**item)
                except TypeError:
                    continue
            self.provider_stats = {}
            for provider, status in data.get("providers", {}).items():
                if not isinstance(status, dict):
                    continue
                try:
                    self.provider_stats[provider] = ProviderBackfillStatus(**status)
                except TypeError:
                    continue

    def save_state(self) -> None:
        with self._lock:
            self.state_path.parent.mkdir(parents=True, exist_ok=True)
            data = {
                "global_paused": self.global_paused,
                "paused_providers": sorted(self.paused_providers),
                "jobs": [job.__dict__ for job in self.jobs.values()],
                "providers": {
                    provider: status.__dict__
                    for provider, status in self.provider_stats.items()
                },
            }
            # Atomic write — never expose a partially-written state file to
            # a concurrent loader after a crash.
            tmp_path = self.state_path.with_suffix(self.state_path.suffix + ".tmp")
            tmp_path.write_text(json.dumps(data, indent=2, sort_keys=True))
            os.replace(tmp_path, self.state_path)
