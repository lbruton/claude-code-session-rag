"""SESF-6 red-phase tests for cleanup/status provider visibility.

Requirements: 6.6, 7, 8.
"""

import importlib
import io
import json
from urllib import error


def test_cleanup_parser_supports_provider_status_and_backfill_controls(stub_rag_engine):
    cleanup = importlib.import_module("cleanup")

    parser = cleanup.build_parser()
    status_args = parser.parse_args(["status", "--provider", "codex"])
    pause_args = parser.parse_args(["backfill", "pause", "--provider", "opencode"])
    resume_args = parser.parse_args(["backfill", "resume", "--provider", "opencode"])

    assert status_args.command == "status"
    assert status_args.provider == "codex"
    assert pause_args.action == "pause"
    assert resume_args.action == "resume"


def test_cleanup_status_output_includes_provider_counts_and_embedding_identity(stub_rag_engine, capsys):
    cleanup = importlib.import_module("cleanup")

    cleanup.cmd_status(type("Args", (), {"project": None, "provider": None})())
    output = capsys.readouterr().out

    assert "Provider" in output
    assert "Embedding" in output
    assert "Backfill" in output


def test_cleanup_server_url_rejects_malformed_port(stub_rag_engine, monkeypatch):
    cleanup = importlib.import_module("cleanup")

    monkeypatch.setenv("SESSIONFLOW_PORT", "7102@attacker.example")
    assert cleanup.get_server_url() == "http://127.0.0.1:7102"

    monkeypatch.setenv("SESSIONFLOW_PORT", "65536")
    assert cleanup.get_server_url() == "http://127.0.0.1:7102"

    monkeypatch.setenv("SESSIONFLOW_PORT", "7103")
    assert cleanup.get_server_url() == "http://127.0.0.1:7103"


def test_cleanup_enqueue_prefers_running_server(stub_rag_engine, tmp_path, monkeypatch, capsys):
    cleanup = importlib.import_module("cleanup")
    monkeypatch.setenv("HOME", str(tmp_path))
    posted = []

    def fake_post(payload):
        posted.append(payload)
        return {"jobs": []}

    monkeypatch.setattr(cleanup, "post_backfill_action", fake_post)

    result = cleanup.cmd_backfill(type("Args", (), {
        "action": "enqueue",
        "provider": "opencode",
        "mode": "full",
    })())

    output = capsys.readouterr().out
    assert result == 0
    assert posted == [{"action": "enqueue", "provider": "opencode", "mode": "full"}]
    assert "via running server" in output


def test_cleanup_enqueue_falls_back_to_local_state(stub_rag_engine, tmp_path, monkeypatch, capsys):
    cleanup = importlib.import_module("cleanup")
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setattr(cleanup, "post_backfill_action", lambda payload: (_ for _ in ()).throw(OSError("down")))

    result = cleanup.cmd_backfill(type("Args", (), {
        "action": "enqueue",
        "provider": "opencode",
        "mode": "full",
    })())

    captured = capsys.readouterr()
    assert result == 0
    assert "falling back to local state file" in captured.err
    assert "Backfill enqueued locally" in captured.out


def test_cleanup_enqueue_server_rejection_fails(stub_rag_engine, tmp_path, monkeypatch, capsys):
    cleanup = importlib.import_module("cleanup")
    monkeypatch.setenv("HOME", str(tmp_path))

    def reject(payload):
        raise error.HTTPError(
            "http://127.0.0.1:7102/backfill",
            400,
            "Bad Request",
            hdrs=None,
            fp=io.BytesIO(b'{"detail":"Unknown provider: bad"}'),
        )

    monkeypatch.setattr(cleanup, "post_backfill_action", reject)

    result = cleanup.cmd_backfill(type("Args", (), {
        "action": "enqueue",
        "provider": "bad",
        "mode": "full",
    })())

    captured = capsys.readouterr()
    assert result == 1
    assert "Server rejected backfill enqueue request: Unknown provider: bad" in captured.err
    assert "falling back" not in captured.err


def test_cleanup_enqueue_malformed_server_response_fails(stub_rag_engine, tmp_path, monkeypatch, capsys):
    cleanup = importlib.import_module("cleanup")
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setattr(
        cleanup,
        "post_backfill_action",
        lambda payload: (_ for _ in ()).throw(json.JSONDecodeError("bad json", "not-json", 0)),
    )

    result = cleanup.cmd_backfill(type("Args", (), {
        "action": "enqueue",
        "provider": "opencode",
        "mode": "full",
    })())

    captured = capsys.readouterr()
    assert result == 1
    assert "Backfill enqueue failed" in captured.err
    assert "falling back" not in captured.err


def test_cleanup_run_prefers_running_server(stub_rag_engine, tmp_path, monkeypatch, capsys):
    cleanup = importlib.import_module("cleanup")
    monkeypatch.setenv("HOME", str(tmp_path))
    posted = []

    def fake_post(payload):
        posted.append(payload)
        return {
            "run": {
                "totals": {
                    "jobs": 2,
                    "processed_sources": 3,
                    "indexed_turns": 4,
                    "errors": 0,
                },
                "skipped": ["bad_provider"],
            }
        }

    monkeypatch.setattr(cleanup, "post_backfill_action", fake_post)

    result = cleanup.cmd_backfill(type("Args", (), {
        "action": "run",
        "mode": "incremental",
        "providers": "opencode,bad_provider",
    })())

    output = capsys.readouterr().out
    assert result == 0
    assert posted == [{
        "action": "run",
        "mode": "incremental",
        "providers": "opencode,bad_provider",
    }]
    assert "Backfill run complete via running server" in output
    assert "jobs=2" in output
    assert "skipped=bad_provider" in output


def test_cleanup_run_server_rejection_fails(stub_rag_engine, tmp_path, monkeypatch, capsys):
    cleanup = importlib.import_module("cleanup")
    monkeypatch.setenv("HOME", str(tmp_path))

    def reject(payload):
        raise error.HTTPError(
            "http://127.0.0.1:7102/backfill",
            400,
            "Bad Request",
            hdrs=None,
            fp=io.BytesIO(b'{"error":"Invalid mode"}'),
        )

    monkeypatch.setattr(cleanup, "post_backfill_action", reject)

    result = cleanup.cmd_backfill(type("Args", (), {
        "action": "run",
        "mode": "bogus",
        "providers": "opencode",
    })())

    captured = capsys.readouterr()
    assert result == 1
    assert "Server rejected backfill run request: Invalid mode" in captured.err
    assert "falling back" not in captured.err
