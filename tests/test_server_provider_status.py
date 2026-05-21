"""SESF-6 red-phase tests for HTTP health/backfill status surfaces.

Requirements: 6.6, 6.7, 7, 8.
"""

import importlib
from contextlib import contextmanager

import pytest


@pytest.mark.anyio
async def test_health_payload_reports_providers_queue_and_local_embedding(stub_rag_engine):
    http_server = importlib.import_module("http_server")

    response = await http_server.health(None)
    payload = response.body.decode("utf-8")

    assert "providers" in payload
    assert "backfill" in payload
    assert "embedding" in payload
    assert "local_mlx" in payload


def test_local_backfill_control_endpoint_exists(stub_rag_engine):
    http_server = importlib.import_module("http_server")

    routes = {getattr(route, "path", "") for route in http_server.app.routes}

    assert "/backfill" in routes or "/backfill/control" in routes


@contextmanager
def _backfill_client(stub_rag_engine, tmp_path, monkeypatch):
    """Reload http_server with the rag_engine stub in place and wrap it in a
    Starlette TestClient. Each test gets a fresh import so /backfill state
    can't leak across tests."""
    import sys

    from starlette.testclient import TestClient

    monkeypatch.setenv("HOME", str(tmp_path))
    # Force a fresh module so the in-process _backfill_manager is reset.
    sys.modules.pop("http_server", None)
    http_server = importlib.import_module("http_server")
    client = TestClient(http_server.app)
    try:
        yield http_server, client
    finally:
        client.close()


def test_backfill_pause_endpoint_pauses_provider(stub_rag_engine, tmp_path, monkeypatch):
    with _backfill_client(stub_rag_engine, tmp_path, monkeypatch) as (http_server, client):
        resp = client.post("/backfill", json={"action": "pause", "provider": "codex"})
        assert resp.status_code == 200
        assert "codex" in http_server._backfill_manager.paused_providers


def test_backfill_enqueue_endpoint_queues_recent_mode(stub_rag_engine, tmp_path, monkeypatch):
    with _backfill_client(stub_rag_engine, tmp_path, monkeypatch) as (http_server, client):
        resp = client.post(
            "/backfill",
            json={"action": "enqueue", "provider": "opencode", "mode": "recent"},
        )
        assert resp.status_code == 200
        queued = http_server._backfill_manager.status().jobs
        assert any(j.provider == "opencode" and j.mode == "recent" for j in queued)


def test_backfill_unknown_action_returns_400(stub_rag_engine, tmp_path, monkeypatch):
    with _backfill_client(stub_rag_engine, tmp_path, monkeypatch) as (_, client):
        resp = client.post("/backfill", json={"action": "garbage"})
        assert resp.status_code == 400


def test_backfill_enqueue_rejects_non_integer_priority(stub_rag_engine, tmp_path, monkeypatch):
    with _backfill_client(stub_rag_engine, tmp_path, monkeypatch) as (_, client):
        resp = client.post(
            "/backfill",
            json={
                "action": "enqueue",
                "provider": "codex",
                "priority": "not-a-number",
            },
        )
        assert resp.status_code == 400
