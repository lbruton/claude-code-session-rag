"""SESF-6 red-phase tests for Antigravity JSONL transcript ingestion.

Requirements: 1, 4, 5, 8.
"""


def test_antigravity_cli_adapter_maps_history_workspace_and_jsonl_steps(synthetic_antigravity_home, tmp_path):
    from provider_antigravity import AntigravityAdapter

    adapter = AntigravityAdapter(home=synthetic_antigravity_home, source_kind="cli")
    source = adapter.discover_sources()[0]
    parsed = adapter.parse_source(source, cursor=None)

    assert source.provider == "antigravity_cli"
    assert source.source_kind == "antigravity_cli_transcript_jsonl"
    assert source.project_root == str(tmp_path)
    assert parsed.turns
    assert parsed.turns[0]["turn_index"] == 1
    assert parsed.turns[0]["provider"] == "antigravity_cli"
    assert parsed.cursor["cursor_type"] == "step_index"


def test_antigravity_adapter_does_not_claim_protobuf_support(tmp_path):
    from provider_antigravity import AntigravityAdapter

    home = tmp_path / "home"
    brain = home / ".gemini" / "antigravity-cli" / "brain" / "opaque"
    brain.mkdir(parents=True)
    (brain / "conversation.pb").write_bytes(b"\x00opaque")

    health = AntigravityAdapter(home=home, source_kind="cli").health()

    assert health.provider == "antigravity_cli"
    assert "protobuf" in " ".join(health.limitations).lower()
