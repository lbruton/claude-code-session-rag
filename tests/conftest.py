"""Shared fixtures for SessionFlow tests."""

import json
import os
import sys
import time
import types
from pathlib import Path

import pytest

# Add project root to sys.path so `import http_server` works.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


@pytest.fixture
def tmp_server_dir(tmp_path):
    """
    Create a temporary `.sessionflow` directory under the provided `tmp_path`.
    
    Returns:
        pathlib.Path: Path to the created `.sessionflow` directory.
    """
    server_dir = tmp_path / ".sessionflow"
    server_dir.mkdir()
    return server_dir


@pytest.fixture
def mock_heartbeat_file(tmp_server_dir):
    """
    Create a heartbeat JSON file with the current timestamp, current process PID, and activity "idle" inside the provided server directory.
    
    Parameters:
        tmp_server_dir (pathlib.Path): Directory in which to create the `heartbeat` file.
    
    Returns:
        pathlib.Path: Path to the created `heartbeat` file.
    """
    hb_path = tmp_server_dir / "heartbeat"
    data = {
        "timestamp": time.time(),
        "pid": os.getpid(),
        "activity": "idle",
    }
    hb_path.write_text(json.dumps(data))
    return hb_path


@pytest.fixture
def stale_heartbeat_file(tmp_server_dir):
    """
    Create a heartbeat file with its timestamp set 300 seconds in the past.
    
    Parameters:
        tmp_server_dir (pathlib.Path): Directory in which to create the `heartbeat` file.
    
    Returns:
        pathlib.Path: Path to the created `heartbeat` file containing JSON with keys `timestamp` (float), `pid` (int), and `activity` (str).
    """
    hb_path = tmp_server_dir / "heartbeat"
    data = {
        "timestamp": time.time() - 300,
        "pid": os.getpid(),
        "activity": "idle",
    }
    hb_path.write_text(json.dumps(data))
    return hb_path


@pytest.fixture
def mock_pid_file(tmp_server_dir):
    """
    Create a server PID file named `server.pid` inside the given directory containing the current process PID.
    
    Parameters:
        tmp_server_dir (pathlib.Path): Directory in which to create the `server.pid` file.
    
    Returns:
        pathlib.Path: Path to the created `server.pid` file.
    """
    pid_path = tmp_server_dir / "server.pid"
    pid_path.write_text(str(os.getpid()))
    return pid_path


@pytest.fixture
def script_path():
    """
    Compute the absolute filesystem path to the `sessionflow-server.sh` script located at the project root.
    
    Returns:
        script_path (str): Absolute path to `sessionflow-server.sh`.
    """
    return str(Path(__file__).resolve().parent.parent / "sessionflow-server.sh")


@pytest.fixture
def synthetic_claude_jsonl(tmp_path):
    """Create a small Claude Code transcript with no real user transcript content."""
    transcript = tmp_path / "claude-session.jsonl"
    entries = [
        {
            "type": "user",
            "cwd": str(tmp_path),
            "gitBranch": "main",
            "timestamp": "2026-05-21T10:00:00Z",
            "message": {"content": "synthetic user request"},
        },
        {
            "type": "assistant",
            "timestamp": "2026-05-21T10:00:01Z",
            "message": {"content": [{"type": "text", "text": "synthetic assistant response"}]},
        },
    ]
    transcript.write_text("\n".join(json.dumps(entry) for entry in entries) + "\n")
    return transcript


@pytest.fixture
def synthetic_codex_home(tmp_path):
    """Create active and archived Codex rollout files for one logical session."""
    home = tmp_path / "home"
    active = home / ".codex" / "sessions" / "2026" / "05" / "21"
    archive = home / ".codex" / "archived_sessions"
    active.mkdir(parents=True)
    archive.mkdir(parents=True)
    active_rollout = active / "rollout-synthetic-session.jsonl"
    archived_rollout = archive / "rollout-synthetic-session.jsonl"
    lines = [
        {
            "timestamp": "2026-05-21T10:00:00Z",
            "type": "session_meta",
            "payload": {"id": "synthetic-session", "cwd": str(tmp_path)},
        },
        {
            "timestamp": "2026-05-21T10:00:01Z",
            "type": "response_item",
            "payload": {
                "type": "message",
                "role": "user",
                "content": [{"type": "input_text", "text": "synthetic codex question"}],
            },
        },
        {
            "timestamp": "2026-05-21T10:00:02Z",
            "type": "response_item",
            "payload": {
                "type": "message",
                "role": "assistant",
                "content": [{"type": "output_text", "text": "synthetic codex answer"}],
            },
        },
    ]
    payload = "\n".join(json.dumps(line) for line in lines) + "\n"
    active_rollout.write_text(payload)
    archived_rollout.write_text(payload)
    return home


@pytest.fixture
def synthetic_opencode_storage(tmp_path):
    """Create a minimal OpenCode storage tree with one message and one part."""
    storage = tmp_path / ".local" / "share" / "opencode" / "storage"
    for name in ("session", "message", "part"):
        (storage / name).mkdir(parents=True)
    (storage / "session" / "synthetic-session.json").write_text(json.dumps({
        "id": "synthetic-session",
        "cwd": str(tmp_path),
        "time": {"created": "2026-05-21T10:00:00Z"},
    }))
    (storage / "message" / "synthetic-message.json").write_text(json.dumps({
        "id": "synthetic-message",
        "sessionID": "synthetic-session",
        "role": "user",
        "time": {"created": "2026-05-21T10:00:01Z"},
    }))
    part = storage / "part" / "synthetic-part.json"
    part.write_text(json.dumps({
        "id": "synthetic-part",
        "sessionID": "synthetic-session",
        "messageID": "synthetic-message",
        "type": "text",
        "text": "synthetic opencode content",
    }))
    return storage


@pytest.fixture
def synthetic_antigravity_home(tmp_path):
    """Create Antigravity CLI history plus JSONL transcript logs."""
    home = tmp_path / "home"
    brain = home / ".gemini" / "antigravity-cli" / "brain" / "synthetic-conversation"
    logs = brain / ".system_generated" / "logs"
    logs.mkdir(parents=True)
    history = home / ".gemini" / "antigravity-cli" / "history.jsonl"
    history.parent.mkdir(parents=True, exist_ok=True)
    history.write_text(json.dumps({
        "conversation_id": "synthetic-conversation",
        "workspace": str(tmp_path),
    }) + "\n")
    (logs / "transcript.jsonl").write_text("\n".join([
        json.dumps({"step_index": 1, "type": "USER_INPUT", "text": "synthetic antigravity ask"}),
        json.dumps({"step_index": 2, "type": "PLANNER_RESPONSE", "text": "synthetic antigravity plan"}),
    ]) + "\n")
    return home


@pytest.fixture
def stub_rag_engine(monkeypatch):
    """Stub the heavy rag_engine import for tests that inspect CLI/server formatting."""
    module = types.SimpleNamespace(
        search=lambda *args, **kwargs: [],
        get_turns=lambda *args, **kwargs: [],
        get_stats=lambda *args, **kwargs: {
            "total_turns": 0,
            "sessions": 0,
            "by_type": {},
            "providers": {},
        },
        delete_older_than=lambda *args, **kwargs: 0,
        delete_by_session=lambda *args, **kwargs: 0,
        delete_by_branch=lambda *args, **kwargs: 0,
        list_sessions=lambda *args, **kwargs: [],
        clear_collection=lambda *args, **kwargs: None,
        init_server_mode=lambda *args, **kwargs: None,
        close_server_mode=lambda *args, **kwargs: None,
        backfill_fts=lambda *args, **kwargs: 0,
        get_model=lambda *args, **kwargs: object(),
        get_model_name=lambda *args, **kwargs: "embeddinggemma",
    )
    monkeypatch.setitem(sys.modules, "rag_engine", module)
    return module
