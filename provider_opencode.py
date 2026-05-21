"""OpenCode storage provider adapter."""

from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional
import hashlib
import json
import time

from provider_adapters import (
    ProviderHealth,
    ProviderParseResult,
    ProviderSource,
    ProviderWatchRoot,
    build_source_id,
    canonicalize_path,
)


class OpenCodeAdapter:
    provider = "opencode"
    source_kind = "opencode_storage"

    def __init__(self, storage_root: str | Path | None = None, settled_seconds: int = 5):
        self.storage_root = (
            Path(storage_root).expanduser()
            if storage_root is not None
            else Path.home() / ".local" / "share" / "opencode" / "storage"
        )
        self.settled_seconds = settled_seconds
        # Per-pass caches populated by _refresh_indexes(). One walk of
        # message/ and part/ replaces the O(N*M) per-session rescans.
        self._messages_by_session: Dict[str, List[tuple[Path, Dict]]] = {}
        self._parts_by_session: Dict[str, List[tuple[Path, Dict]]] = {}
        self._all_message_ids: set[str] = set()
        self._all_parts: List[tuple[Path, Dict]] = []
        self._indexes_built = False

    def _refresh_indexes(self) -> None:
        """Walk message/ and part/ once and bucket records by sessionID."""
        self._messages_by_session = {}
        self._parts_by_session = {}
        self._all_message_ids = set()
        self._all_parts = []

        message_root = self.storage_root / "message"
        if message_root.exists():
            for path in sorted(message_root.rglob("*.json")):
                data = self._load_json(path)
                session_id = data.get("sessionID") or data.get("session_id")
                if session_id:
                    self._messages_by_session.setdefault(str(session_id), []).append((path, data))
                msg_id = data.get("id")
                if msg_id:
                    self._all_message_ids.add(str(msg_id))

        part_root = self.storage_root / "part"
        if part_root.exists():
            for path in sorted(part_root.rglob("*.json")):
                data = self._load_json(path)
                self._all_parts.append((path, data))
                session_id = data.get("sessionID") or data.get("session_id")
                if session_id:
                    self._parts_by_session.setdefault(str(session_id), []).append((path, data))
        self._indexes_built = True

    def _clear_indexes(self) -> None:
        self._messages_by_session = {}
        self._parts_by_session = {}
        self._all_message_ids = set()
        self._all_parts = []
        self._indexes_built = False

    def _load_json(self, path: Path) -> Dict:
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}

    def _session_files(self) -> List[Path]:
        root = self.storage_root / "session"
        return sorted(root.rglob("*.json")) if root.exists() else []

    def discover_sources(self) -> List[ProviderSource]:
        # Build per-pass indexes once; reused by every parse_source() call
        # against the same adapter instance until the next discover_sources().
        self._refresh_indexes()
        sources = []
        for path in self._session_files():
            data = self._load_json(path)
            logical_session_id = str(data.get("id") or path.stem)
            canonical_path = canonicalize_path(path)
            source_id = build_source_id(self.provider, logical_session_id, canonical_path)
            created = data.get("time", {}).get("created", "") if isinstance(data.get("time"), dict) else ""
            sources.append(ProviderSource(
                provider=self.provider,
                source_kind=self.source_kind,
                source_class="native",
                source_id=source_id,
                logical_session_id=logical_session_id,
                path=str(path),
                canonical_path=canonical_path,
                project_root=data.get("cwd") or data.get("directory") or "unknown",
                timestamp=created,
                status="eligible",
            ))
        return sources

    def _message_records(self, session_id: str) -> List[tuple[Path, Dict]]:
        if not self._indexes_built:
            self._refresh_indexes()
        return list(self._messages_by_session.get(session_id, []))

    def _part_records(self, session_id: str) -> List[tuple[Path, Dict]]:
        if not self._indexes_built:
            self._refresh_indexes()
        return list(self._parts_by_session.get(session_id, []))

    def _is_settled(self, paths: List[Path]) -> bool:
        if self.settled_seconds <= 0:
            return True
        now = time.time()
        return all(now - path.stat().st_mtime >= self.settled_seconds for path in paths if path.exists())

    def parse_source(
        self,
        source: ProviderSource,
        cursor: Optional[Dict],
    ) -> ProviderParseResult:
        messages = self._message_records(source.logical_session_id)
        parts = self._part_records(source.logical_session_id)
        paths = [Path(source.path)] + [path for path, _ in messages] + [path for path, _ in parts]
        if not self._is_settled(paths):
            source.status = "pending"
            source.reason = "OpenCode records are still inside the settled window."
            return ProviderParseResult(source=source, turns=[], cursor=cursor or {})

        parts_by_message: Dict[str, List[Dict]] = {}
        for _, part in parts:
            message_id = part.get("messageID") or part.get("message_id")
            if message_id:
                parts_by_message.setdefault(message_id, []).append(part)

        emitted = set((cursor or {}).get("emitted_ids", []))
        turns = []
        for index, (_, message) in enumerate(messages):
            message_id = message.get("id", "")
            role = message.get("role", "")
            content_parts = [
                part.get("text", "")
                for part in parts_by_message.get(message_id, [])
                if part.get("type") in {"text", "message"} and part.get("text")
            ]
            if not role or not content_parts:
                continue
            text = "\n".join(content_parts)
            doc_hash = hashlib.sha256(
                f"{source.logical_session_id}:{message_id}:{text}".encode("utf-8")
            ).hexdigest()[:16]
            doc_id = f"opencode:{source.logical_session_id}:{doc_hash}"
            if doc_id in emitted:
                continue
            emitted.add(doc_id)
            timestamp = ""
            if isinstance(message.get("time"), dict):
                timestamp = message["time"].get("created", "")
            turns.append({
                "text": text,
                "content": text,
                "doc_id": doc_id,
                "session_id": source.logical_session_id,
                "logical_session_id": source.logical_session_id,
                "provider": self.provider,
                "source_kind": self.source_kind,
                "source_class": "native",
                "source_id": source.source_id,
                "source_path": source.path,
                "transcript_file": Path(source.path).name,
                "turn_index": index,
                "timestamp": timestamp or source.timestamp,
                "git_branch": "",
                "chunk_type": role or "turn",
                "project_root": source.project_root,
            })

        return ProviderParseResult(
            source=source,
            turns=turns,
            cursor={
                "cursor_type": "record_set",
                "logical_session_id": source.logical_session_id,
                "known_paths": [str(path) for path in paths],
                "emitted_ids": sorted(emitted),
                "project_root": source.project_root,
            },
        )

    def _orphan_part_count(self) -> int:
        if not self._indexes_built:
            self._refresh_indexes()
        count = 0
        for _, data in self._all_parts:
            message_id = data.get("messageID") or data.get("message_id")
            if message_id and str(message_id) not in self._all_message_ids:
                count += 1
        return count

    def watch_roots(self) -> List[ProviderWatchRoot]:
        return [ProviderWatchRoot(self.provider, self.source_kind, str(self.storage_root), recursive=True)]

    def health(self) -> ProviderHealth:
        sources = self.discover_sources()
        orphan_parts = self._orphan_part_count()
        status = "ok" if self.storage_root.exists() and not orphan_parts else "warning"
        if not self.storage_root.exists():
            status = "missing"
        return ProviderHealth(
            provider=self.provider,
            status=status,
            source_count=len(sources),
            eligible_count=len(sources),
            error_count=orphan_parts,
            roots=[str(self.storage_root)],
            limitations=["Incomplete OpenCode part/message records are left pending."] if orphan_parts else [],
        )
