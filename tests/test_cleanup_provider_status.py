"""SESF-6 red-phase tests for cleanup/status provider visibility.

Requirements: 6.6, 7, 8.
"""

import importlib


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
