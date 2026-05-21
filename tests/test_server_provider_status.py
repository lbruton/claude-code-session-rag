"""SESF-6 red-phase tests for HTTP health/backfill status surfaces.

Requirements: 6.6, 6.7, 7, 8.
"""

import importlib

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
