"""Antigravity JSONL transcript provider adapter."""

from __future__ import annotations

import warnings
from pathlib import Path
from typing import Dict, List, Optional
import hashlib
import json

from provider_adapters import (
    ProviderHealth,
    ProviderParseResult,
    ProviderSource,
    ProviderWatchRoot,
    build_source_id,
    canonicalize_path,
    normalize_timestamp,
)


def _walk_length_delimited(data: bytes) -> Dict[str, str]:
    """Decode the Antigravity ``agyhub_summaries_proto.pb`` wire format.

    Schema-free varint / length-delimited walker (stdlib only). Returns a
    ``{conversation_id: workspace_uri}`` map. Stub: parsing logic lands in
    Cohort C (SESF-17 C.1); currently a no-op returning ``{}``.
    """
    return {}


def _normalize_file_uri(value: str) -> Optional[str]:
    """Normalize a ``file://`` URI (or bare absolute path) to a filesystem path.

    Built on ``urllib.parse`` in Cohort C. Stub: returns its input unchanged;
    no decoding or validation logic yet (SESF-17 C.1).
    """
    return value


def _load_summaries(root: Path) -> Dict[str, str]:
    """Load the desktop summaries metadata into a ``{conversation_id: path}`` map.

    Ties ``_walk_length_delimited`` and ``_normalize_file_uri`` together, wrapping
    all file I/O and binary decoding defensively (returns ``{}`` on any failure).
    Stub: no parsing logic yet; always returns ``{}`` (SESF-17 C.1).
    """
    return {}


class AntigravityAdapter:
    def __init__(self, home: str | Path | None = None, source_kind: str = "cli"):
        self.home = Path(home).expanduser() if home is not None else Path.home()
        self.variant = source_kind
        if source_kind == "desktop":
            self.provider = "antigravity_desktop"
            self.source_kind = "antigravity_desktop_transcript_jsonl"
            self.root = self.home / ".gemini" / "antigravity"
        elif source_kind == "cli":
            self.provider = "antigravity_cli"
            self.source_kind = "antigravity_cli_transcript_jsonl"
            self.root = self.home / ".gemini" / "antigravity-cli"
        else:
            raise ValueError(f"unknown source_kind: {source_kind!r}")

    def _load_history(self) -> Dict[str, str]:
        history_path = self.root / "history.jsonl"
        mapping: Dict[str, str] = {}
        if not history_path.exists():
            return mapping
        try:
            lines = history_path.read_text(encoding="utf-8").splitlines()
        except OSError:
            return mapping
        for line in lines:
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                continue
            conversation_id = (
                entry.get("conversation_id")
                or entry.get("conversationId")
                or entry.get("id")
            )
            workspace = entry.get("workspace") or entry.get("cwd") or entry.get("project_root")
            if conversation_id and workspace:
                mapping[str(conversation_id)] = str(workspace)
        return mapping

    def discover_sources(self) -> List[ProviderSource]:
        transcript_glob = "brain/*/.system_generated/logs/transcript.jsonl"
        history = self._load_history()
        # Desktop history.jsonl is empty; consult the summaries metadata as a
        # second resolution layer (SESF-17). Gated to the desktop variant so the
        # CLI path is provably untouched (AC-6). Parsed once per discovery pass,
        # mirroring _load_history(). Stub loader returns {} until Cohort C, so
        # desktop project_root still resolves to "unknown" here.
        summaries: Dict[str, str] = {}
        if self.variant == "desktop":
            summaries = _load_summaries(self.root)
        sources = []
        for path in sorted(self.root.glob(transcript_glob)):
            conversation_id = path.parents[2].name
            canonical_path = canonicalize_path(path)
            source_id = build_source_id(self.provider, conversation_id, canonical_path)
            sources.append(ProviderSource(
                provider=self.provider,
                source_kind=self.source_kind,
                source_class="native",
                source_id=source_id,
                logical_session_id=conversation_id,
                path=str(path),
                canonical_path=canonical_path,
                project_root=history.get(
                    conversation_id, summaries.get(conversation_id, "unknown")
                ),
                timestamp="",
                status="eligible",
            ))
        return sources

    def parse_source(
        self,
        source: ProviderSource,
        cursor: Optional[Dict],
    ) -> ProviderParseResult:
        last_step = int((cursor or {}).get("last_step_index", -1))
        emitted = set((cursor or {}).get("emitted_ids", []))
        turns = []
        try:
            lines = Path(source.path).read_text(encoding="utf-8").splitlines()
        except OSError as exc:
            return ProviderParseResult(source=source, turns=[], cursor=cursor or {}, errors=[str(exc)])

        for line_number, line in enumerate(lines, 1):
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                continue
            # TODO(SESF-6+): step_index can collide across logical sessions if
            # cursor provenance is shared (e.g. resumed conversation with a
            # rewritten transcript). Revisit cursor scoping once we have
            # ground-truth examples of multi-session step_index reuse.
            raw_step = entry.get("step_index")
            raw_step_alt = entry.get("stepIndex")
            try:
                step_index = int(raw_step if raw_step is not None else (
                    raw_step_alt if raw_step_alt is not None else line_number
                ))
            except (TypeError, ValueError):
                warnings.warn(
                    f"provider_antigravity: malformed step_index {raw_step!r}/{raw_step_alt!r}"
                    f" at line {line_number} of {source.path!r}; skipping record",
                    stacklevel=1,
                )
                continue
            if step_index <= last_step:
                continue
            text = entry.get("text") or entry.get("content") or entry.get("message") or ""
            if not text:
                continue
            # Antigravity transcripts use `created_at`; older / desktop variants
            # have also been seen with `timestamp`. Accept either.
            raw_ts = next(
                (
                    v for k in ("timestamp", "created_at", "createdAt", "time")
                    if (v := entry.get(k)) is not None and v != ""
                ),
                None,
            )
            doc_hash = hashlib.sha256(
                f"{source.logical_session_id}:{step_index}:{text}".encode("utf-8")
            ).hexdigest()[:16]
            doc_id = f"{self.provider}:{source.logical_session_id}:{doc_hash}"
            if doc_id in emitted:
                continue
            emitted.add(doc_id)
            turns.append({
                "text": str(text),
                "content": str(text),
                "doc_id": doc_id,
                "session_id": source.logical_session_id,
                "logical_session_id": source.logical_session_id,
                "provider": self.provider,
                "source_kind": self.source_kind,
                "source_class": "native",
                "source_id": source.source_id,
                "source_path": source.path,
                "transcript_file": Path(source.path).name,
                "turn_index": step_index,
                "timestamp": normalize_timestamp(raw_ts) or source.timestamp,
                "git_branch": "",
                "chunk_type": entry.get("type", "turn"),
                "project_root": source.project_root,
            })
            last_step = max(last_step, step_index)

        return ProviderParseResult(
            source=source,
            turns=turns,
            cursor={
                "cursor_type": "step_index",
                "logical_session_id": source.logical_session_id,
                "known_paths": [source.path],
                "last_step_index": last_step,
                "emitted_ids": sorted(emitted),
                "project_root": source.project_root,
            },
        )

    def _has_opaque_binary_artifacts(self) -> bool:
        return any(self.root.glob("brain/**/*.pb")) or any(self.root.glob("brain/**/*.db"))

    def watch_roots(self) -> List[ProviderWatchRoot]:
        return [ProviderWatchRoot(self.provider, self.source_kind, str(self.root / "brain"), recursive=True)]

    def health(self) -> ProviderHealth:
        sources = self.discover_sources()
        limitations = ["Protobuf/database artifacts are not parsed in SESF-6."]
        if self._has_opaque_binary_artifacts():
            limitations.append("Opaque protobuf/database artifacts detected; JSONL remains authoritative.")
        return ProviderHealth(
            provider=self.provider,
            status="ok" if sources else ("missing" if not self.root.exists() else "partial"),
            source_count=len(sources),
            eligible_count=len(sources),
            roots=[str(self.root)],
            limitations=limitations,
        )
