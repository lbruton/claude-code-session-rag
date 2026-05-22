"""SESF-6 red-phase tests for OpenCode storage reconstruction.

Requirements: 3, 5, 6.
"""

import json
import os
import time


def test_opencode_adapter_waits_for_settled_message_parts(synthetic_opencode_storage):
    from provider_opencode import OpenCodeAdapter

    adapter = OpenCodeAdapter(storage_root=synthetic_opencode_storage, settled_seconds=60)
    source = adapter.discover_sources()[0]
    first_parse = adapter.parse_source(source, cursor=None)

    assert first_parse.turns == []
    assert first_parse.source.status == "pending"

    old = time.time() - 120
    for path in synthetic_opencode_storage.rglob("*.json"):
        os.utime(path, (old, old))

    second_parse = adapter.parse_source(source, cursor=None)
    assert second_parse.turns
    assert second_parse.turns[0]["provider"] == "opencode"
    assert second_parse.turns[0]["source_kind"] == "opencode_storage"


def test_opencode_adapter_normalizes_int_ms_timestamps(tmp_path):
    """SESF-14 regression: real OpenCode rollouts store time.created as int ms epoch.

    Without normalization, every insert fails Milvus's VARCHAR(64) timestamp
    schema with DataNotMatchException.
    """
    from provider_opencode import OpenCodeAdapter

    storage = tmp_path / "storage"
    for name in ("session", "message", "part"):
        (storage / name).mkdir(parents=True)

    created_ms = 1747832400000
    msg_ms = 1747832401000

    (storage / "session" / "s.json").write_text(json.dumps({
        "id": "s",
        "cwd": str(tmp_path),
        "time": {"created": created_ms},
    }))
    (storage / "message" / "m.json").write_text(json.dumps({
        "id": "m",
        "sessionID": "s",
        "role": "user",
        "time": {"created": msg_ms},
    }))
    (storage / "part" / "p.json").write_text(json.dumps({
        "id": "p",
        "sessionID": "s",
        "messageID": "m",
        "type": "text",
        "text": "hello",
    }))

    adapter = OpenCodeAdapter(storage_root=storage, settled_seconds=0)
    sources = adapter.discover_sources()
    assert sources, "expected one source"
    assert isinstance(sources[0].timestamp, str)
    assert sources[0].timestamp.startswith("20"), sources[0].timestamp

    result = adapter.parse_source(sources[0], cursor=None)
    assert result.turns, "expected at least one turn"
    for turn in result.turns:
        assert isinstance(turn["timestamp"], str), turn["timestamp"]


def test_opencode_adapter_reports_incomplete_records_without_partial_index(tmp_path):
    from provider_opencode import OpenCodeAdapter

    storage = tmp_path / "storage"
    (storage / "part").mkdir(parents=True)
    (storage / "part" / "orphan.json").write_text('{"messageID":"missing","text":"synthetic"}')

    health = OpenCodeAdapter(storage_root=storage, settled_seconds=0).health()

    assert health.provider == "opencode"
    assert health.status in {"warning", "partial", "error"}
    assert health.error_count >= 1
