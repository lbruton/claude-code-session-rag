"""SESF-6 red-phase tests for Antigravity JSONL transcript ingestion.

Requirements: 1, 4, 5, 8.

SESF-17 (B.1) adds desktop summaries-based project_root resolution tests
below (AC-1..AC-8), all driven by the synthetic_antigravity_desktop_home
fixture. These are RED-phase: the A.2 helper stubs (_walk_length_delimited,
_normalize_file_uri, _load_summaries) are no-ops, so the AC-1/AC-2/AC-3
(summaries-wins)/AC-5(real-parse) assertions fail until Cohort C lands.
"""

import dataclasses
import json


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


# ---------------------------------------------------------------------------
# SESF-17 (B.1) — desktop summaries-based project_root resolution (AC-1..AC-8)
# ---------------------------------------------------------------------------


def _desktop_project_roots(fixture):
    """Discover desktop sources and return {conversation_id: project_root}."""
    from provider_antigravity import AntigravityAdapter

    adapter = AntigravityAdapter(home=fixture["home"], source_kind="desktop")
    return {s.logical_session_id: s.project_root for s in adapter.discover_sources()}


def test_desktop_summaries_maps_conversation_to_workspace_path_ac1(synthetic_antigravity_desktop_home):
    """AC-1: a conversation_id present in the .pb resolves to the mapped workspace PATH."""
    fixture = synthetic_antigravity_desktop_home
    roots = _desktop_project_roots(fixture)

    for conversation_id, workspace_path in fixture["mapped"].items():
        assert roots[conversation_id] == workspace_path


def test_desktop_summaries_normalizes_percent_encoded_file_uri_ac2(synthetic_antigravity_desktop_home):
    """AC-2: a file:// URI with %20/unicode decodes (urlparse+unquote) to the literal path."""
    fixture = synthetic_antigravity_desktop_home
    roots = _desktop_project_roots(fixture)

    # Exact decoded path — not a partial match, not a bare scheme strip.
    assert roots[fixture["encoded_id"]] == fixture["encoded_decoded"]
    assert roots[fixture["encoded_id"]] == "/Users/lbruton/My Projects/café"


def test_desktop_resolution_precedence_history_over_summaries_ac3(synthetic_antigravity_desktop_home):
    """AC-3 (history wins): when both history.jsonl and summaries map a conversation, history wins."""
    from provider_antigravity import AntigravityAdapter

    fixture = synthetic_antigravity_desktop_home
    # Pick a conversation that is mapped in the .pb summaries, then add a
    # *different* workspace for it in history.jsonl. History must take precedence.
    conversation_id = next(iter(fixture["mapped"]))
    history_workspace = "/tmp/history-wins-workspace"
    assert fixture["mapped"][conversation_id] != history_workspace

    history_path = fixture["root"] / "history.jsonl"
    history_path.write_text(
        json.dumps({"conversation_id": conversation_id, "workspace": history_workspace}) + "\n",
        encoding="utf-8",
    )

    adapter = AntigravityAdapter(home=fixture["home"], source_kind="desktop")
    roots = {s.logical_session_id: s.project_root for s in adapter.discover_sources()}

    assert roots[conversation_id] == history_workspace


def test_desktop_resolution_precedence_summaries_when_no_history_ac3(synthetic_antigravity_desktop_home):
    """AC-3 (summaries wins): with no history entry, summaries supplies the workspace."""
    fixture = synthetic_antigravity_desktop_home
    # No history.jsonl is written by the fixture, so summaries is the resolving layer.
    assert not (fixture["root"] / "history.jsonl").exists()

    roots = _desktop_project_roots(fixture)
    conversation_id = next(iter(fixture["mapped"]))

    assert roots[conversation_id] == fixture["mapped"][conversation_id]


def test_desktop_unmapped_conversation_resolves_unknown_ac4(synthetic_antigravity_desktop_home):
    """AC-4: a conversation present in brain but absent from .pb (and history) resolves to "unknown"."""
    fixture = synthetic_antigravity_desktop_home
    roots = _desktop_project_roots(fixture)

    for conversation_id in fixture["unmapped"]:
        assert roots[conversation_id] == "unknown"


def test_desktop_absent_pb_resolves_unknown_without_raising_ac5(synthetic_antigravity_desktop_home):
    """AC-5: an absent .pb -> loader returns {}, desktop sources resolve to "unknown", no raise."""
    fixture = synthetic_antigravity_desktop_home
    fixture["pb_path"].unlink()

    roots = _desktop_project_roots(fixture)

    for conversation_id in fixture["mapped"]:
        assert roots[conversation_id] == "unknown"
    for conversation_id in fixture["unmapped"]:
        assert roots[conversation_id] == "unknown"


def test_desktop_truncated_pb_resolves_unknown_without_raising_ac5(synthetic_antigravity_desktop_home):
    """AC-5: a truncated/boundary-error .pb -> graceful "unknown", no raise."""
    fixture = synthetic_antigravity_desktop_home
    # Replace the canonical .pb with the malformed/truncated bytes from the fixture.
    fixture["pb_path"].write_bytes(fixture["truncated_pb_path"].read_bytes())

    roots = _desktop_project_roots(fixture)

    for conversation_id in fixture["mapped"]:
        assert roots[conversation_id] == "unknown"


def test_desktop_unreadable_pb_resolves_unknown_without_raising_ac5(synthetic_antigravity_desktop_home, monkeypatch):
    """AC-5: an unreadable .pb (PermissionError/OSError) -> graceful "unknown", no raise."""
    import pathlib

    fixture = synthetic_antigravity_desktop_home
    pb_path = fixture["pb_path"]
    real_read_bytes = pathlib.Path.read_bytes

    def boom(self, *args, **kwargs):
        if self == pb_path:
            raise PermissionError("locked by concurrent desktop-daemon write")
        return real_read_bytes(self, *args, **kwargs)

    monkeypatch.setattr(pathlib.Path, "read_bytes", boom)

    roots = _desktop_project_roots(fixture)

    for conversation_id in fixture["mapped"]:
        assert roots[conversation_id] == "unknown"


def test_desktop_decode_error_pb_resolves_unknown_without_raising_ac5(synthetic_antigravity_desktop_home, monkeypatch):
    """AC-5: a UnicodeDecodeError / boundary error during decode -> graceful "unknown", no raise."""
    import provider_antigravity

    fixture = synthetic_antigravity_desktop_home

    def boom(data):
        raise UnicodeDecodeError("utf-8", b"", 0, 1, "synthetic decode boundary error")

    monkeypatch.setattr(provider_antigravity, "_walk_length_delimited", boom)

    roots = _desktop_project_roots(fixture)

    for conversation_id in fixture["mapped"]:
        assert roots[conversation_id] == "unknown"


def test_cli_variant_resolution_unchanged_no_summaries_ac6(synthetic_antigravity_home, tmp_path, monkeypatch):
    """AC-6: the antigravity_cli variant resolves from history only; summaries is never consulted."""
    import provider_antigravity
    from provider_antigravity import AntigravityAdapter

    def fail_if_called(root):
        raise AssertionError("CLI variant must not consult summaries metadata")

    monkeypatch.setattr(provider_antigravity, "_load_summaries", fail_if_called)

    adapter = AntigravityAdapter(home=synthetic_antigravity_home, source_kind="cli")
    sources = adapter.discover_sources()

    assert sources[0].provider == "antigravity_cli"
    # Identical to pre-sketch behavior: history.jsonl supplies the workspace.
    assert sources[0].project_root == str(tmp_path)


def test_project_root_remains_str_path_no_project_name_field_ac7(synthetic_antigravity_desktop_home):
    """AC-7: project_root stays a str path; no new project-name field; junk -> "unknown" (D-6)."""
    from provider_antigravity import AntigravityAdapter

    fixture = synthetic_antigravity_desktop_home
    adapter = AntigravityAdapter(home=fixture["home"], source_kind="desktop")
    sources = adapter.discover_sources()

    field_names = {f.name for f in dataclasses.fields(sources[0])}
    # No new project-name field is introduced; the path field is the only carrier.
    assert "project_root" in field_names
    assert not any(
        "project_name" in name or "project_display" in name or name == "workspace_name"
        for name in field_names
    )

    for source in sources:
        assert isinstance(source.project_root, str)
        # Every value is either an absolute path or the literal "unknown" sentinel —
        # never a derived name (D-6).
        assert source.project_root == "unknown" or source.project_root.startswith("/")

    # Mapped conversations carry absolute paths, not derived names.
    roots = {s.logical_session_id: s.project_root for s in sources}
    for conversation_id, workspace_path in fixture["mapped"].items():
        assert roots[conversation_id].startswith("/")


def test_desktop_health_reflects_summaries_parsed_ac8(synthetic_antigravity_desktop_home):
    """AC-8 (desktop): limitations no longer claim summaries is unparsed, still names opaque brain artifacts."""
    from provider_antigravity import AntigravityAdapter

    fixture = synthetic_antigravity_desktop_home
    health = AntigravityAdapter(home=fixture["home"], source_kind="desktop").health()
    text = " ".join(health.limitations)
    lowered = text.lower()

    assert health.provider == "antigravity_desktop"
    # Must no longer report the summaries metadata as unparsed.
    assert "Protobuf/database artifacts are not parsed in SESF-6." not in text
    # Still names the genuinely-opaque per-conversation brain/**/*.pb / *.db artifacts.
    assert "brain" in lowered
    assert ".pb" in lowered or "protobuf" in lowered or ".db" in lowered


def test_cli_health_keeps_baseline_protobuf_string_ac8(synthetic_antigravity_home):
    """AC-8 (CLI): the CLI variant keeps the baseline limitations string unchanged."""
    from provider_antigravity import AntigravityAdapter

    health = AntigravityAdapter(home=synthetic_antigravity_home, source_kind="cli").health()

    assert health.provider == "antigravity_cli"
    assert "Protobuf/database artifacts are not parsed in SESF-6." in health.limitations
