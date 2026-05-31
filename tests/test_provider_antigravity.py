"""Tests for Antigravity JSONL transcript ingestion (SESF-6) and desktop
summaries-based project_root resolution (SESF-17).

The SESF-6 section covers CLI adapter wiring, history.jsonl workspace mapping,
and the protobuf-support health baseline.

The SESF-17 section (AC-1..AC-8) verifies desktop summaries project_root
resolution via the implemented helpers (_read_varint, _iter_length_delimited,
_walk_length_delimited, _normalize_file_uri, _load_summaries). All helpers are
fully implemented and the suite is green. Tests cover:
  AC-1: .pb-mapped conversation resolves to workspace path.
  AC-2: percent-encoded file:// URI (urlparse+unquote) decodes correctly.
  AC-3: history.jsonl wins over summaries; summaries wins when history absent.
  AC-4: unmapped conversation resolves to "unknown".
  AC-5: absent/truncated/unreadable/decode-error .pb degrades gracefully (no raise).
  AC-6: CLI variant never consults summaries.
  AC-7: project_root is a str path; no project-name field; junk -> "unknown".
  AC-8: desktop health limitations reflect parsed summaries; CLI keeps baseline.
Additionally: wire-type robustness — records with interleaved wt=0/1/5 fields
and reversed field order (Field 2 before Field 1) still resolve correctly.
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
    for conversation_id in fixture["mapped"]:
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


def test_desktop_summaries_skips_non_length_delimited_wire_types(
    synthetic_antigravity_desktop_home,
):
    """Wire-type robustness: a record that embeds wt=0/1/5 fields and reversed field order
    must still resolve to its workspace path.

    Before Fix 1+2, _iter_length_delimited raises ValueError on the first wt!=2 tag, which
    causes _load_summaries to return {} and the conversation resolves to "unknown".
    After the fix, the walker skips wt=0/1/5 fields and _walk_length_delimited collects
    both Field 1 and Field 2 regardless of arrival order, so the workspace path resolves.
    """
    fixture = synthetic_antigravity_desktop_home
    roots = _desktop_project_roots(fixture)

    assert roots[fixture["mixed_wire_id"]] == fixture["mixed_wire_ws"]


def test_desktop_summaries_isolates_single_malformed_record():
    """Per-record fault isolation (SESF-29 review): a single corrupt record inside
    an otherwise-valid .pb is skipped, not allowed to discard every other mapping.

    The malformed record's outer (top-level Field 1) frame is well-formed, so the
    top-level walker still hands its payload to _decode_record; the *inner* bytes
    claim a length-delimited field longer than the buffer, raising IndexError. With
    fault isolation that record is dropped and the two valid records still resolve;
    without it, _iter_length_delimited's error would bubble to _load_summaries and
    zero out the whole file.
    """
    import provider_antigravity

    def _varint(value):
        out = bytearray()
        while True:
            byte = value & 0x7F
            value >>= 7
            if value:
                out.append(byte | 0x80)
            else:
                out.append(byte)
                return bytes(out)

    def _ld(tag, payload):  # length-delimited (wire type 2) field
        return bytes([tag]) + _varint(len(payload)) + payload

    def _record(conversation_id, uri):  # inner body: F1=uuid, F2->9->1=uri
        nested = _ld(0x0A, uri.encode("utf-8"))
        return _ld(0x0A, conversation_id.encode("utf-8")) + _ld(0x12, _ld(0x4A, nested))

    def _wrap(record):  # top-level Field 1 frame
        return _ld(0x0A, record)

    good_a_id = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    good_b_id = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
    good_a = _wrap(_record(good_a_id, "file:///tmp/a"))
    good_b = _wrap(_record(good_b_id, "file:///tmp/b"))
    # Inner Field 1 (wt=2) declares a 50-byte payload but only 3 bytes follow.
    malformed = _wrap(bytes([0x0A]) + _varint(50) + b"abc")

    mapping = provider_antigravity._walk_length_delimited(good_a + malformed + good_b)

    assert mapping == {good_a_id: "file:///tmp/a", good_b_id: "file:///tmp/b"}
