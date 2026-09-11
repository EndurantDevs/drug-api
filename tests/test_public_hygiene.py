import hashlib
import json
import re
from pathlib import Path

import pytest

from scripts.ci import public_hygiene as hygiene


@pytest.fixture
def synthetic_policy(monkeypatch):
    monkeypatch.setattr(
        hygiene,
        "PRIVATE_INTEGRATION_FINGERPRINTS",
        {hashlib.sha256(b"samplewidget").hexdigest()},
    )
    return hygiene


@pytest.mark.parametrize("text", ["sample-widget", "SAMPLE_WIDGET", "https://example.test/sample/widget"])
def test_fingerprints_normalize_identifier_separators(synthetic_policy, text):
    assert synthetic_policy.check_text(text, "PR body") == ["private-integration-fingerprint: PR body"]


def test_compatibility_exception_is_exact_and_does_not_hide_other_content(synthetic_policy, monkeypatch):
    monkeypatch.setattr(
        synthetic_policy,
        "PUBLIC_IDENTIFIER_RE",
        re.compile(r"(?<![A-Za-z0-9_./:\\-])SAMPLE_WIDGET_URL(?![A-Za-z0-9_./:\\-])"),
    )
    assert synthetic_policy.check_text("`SAMPLE_WIDGET_URL`", "PR body") == []
    for text in (
        "SAMPLE_WIDGET_URL and sample-widget",
        "SAMPLE_WIDGET_URL and https://example.test/sample-widget",
        "prefix_SAMPLE_WIDGET_URL",
        "SAMPLE_WIDGET_URL_SUFFIX",
    ):
        assert synthetic_policy.check_text(text, "PR body")


def test_file_diagnostics_never_echo_rejected_names(synthetic_policy, tmp_path):
    candidate = tmp_path / "sample-widget.txt"
    candidate.write_text("sample-widget", encoding="utf-8")
    errors = synthetic_policy.check_paths([candidate]) + synthetic_policy.check_content([candidate])
    assert errors == ["private-path-fingerprint: file 1", "private-integration-fingerprint: file 1"]
    assert not any(str(candidate) in error or "sample-widget" in error for error in errors)


@pytest.mark.parametrize("field", ["title", "body", "ref"])
def test_pull_request_metadata_checks_each_field(synthetic_policy, tmp_path, field):
    request_dict = {"title": "fix: validate content", "body": None, "head": {"ref": "fix/content"}}
    if field == "ref":
        request_dict["head"][field] = "fix/sample-widget"
    else:
        request_dict[field] = "sample-widget"
    event = tmp_path / "event.json"
    event.write_text(json.dumps({"pull_request": request_dict}), encoding="utf-8")
    assert len(synthetic_policy.check_event(event)) == 1


def test_push_metadata_checks_complete_messages_and_head(synthetic_policy, tmp_path):
    event = tmp_path / "event.json"
    event.write_text(
        json.dumps(
            {
                "ref": "refs/heads/dev",
                "commits": [{"message": "fix: validate content\n\nsample-widget"}],
                "head_commit": {"message": "fix: validate content\n\nsample-widget"},
            }
        ),
        encoding="utf-8",
    )
    assert synthetic_policy.check_event(event) == [
        "private-integration-fingerprint: push commit 1",
        "private-integration-fingerprint: push head commit",
    ]


@pytest.mark.parametrize(
    "payload",
    [
        [],
        {},
        {"pull_request": {"title": "fix: validate content"}},
        {"pull_request": {"title": "fix: validate content", "body": [], "head": {"ref": "fix/content"}}},
        {"ref": "refs/heads/dev", "commits": []},
        {"ref": "refs/heads/dev", "commits": [{"message": None}]},
        {"ref": "refs/heads/dev", "commits": [{"message": " "}]},
        {"ref": "refs/heads/dev", "commits": [{"message": "body\0text"}]},
        {"ref": "refs/heads/dev\nextra", "commits": [{"message": "fix: validate content"}]},
        {"ref": "refs/heads/dev", "commits": [{"message": "fix: validate content"}], "head_commit": {}},
    ],
)
def test_malformed_metadata_fails_closed(tmp_path, payload):
    event = tmp_path / "event.json"
    event.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="missing or malformed"):
        hygiene.event_texts(event)


def test_cli_checks_repeated_text_files_and_ambient_event(synthetic_policy, monkeypatch, tmp_path, capsys):
    monkeypatch.setattr(synthetic_policy, "repository_files", lambda **kwargs: [])
    event = tmp_path / "event.json"
    event.write_text(
        json.dumps({"pull_request": {"title": "fix: validate content", "body": "", "head": {"ref": "fix/content"}}}),
        encoding="utf-8",
    )
    monkeypatch.setenv("GITHUB_EVENT_NAME", "pull_request")
    monkeypatch.setenv("GITHUB_EVENT_PATH", str(event))
    allowed = tmp_path / "title.txt"
    allowed.write_text("fix: validate content", encoding="utf-8")
    rejected = tmp_path / "sample-widget.txt"
    rejected.write_text("sample-widget", encoding="utf-8")
    assert synthetic_policy.main(["--text-file", str(allowed), "--text-file", str(rejected)]) == 1
    output = capsys.readouterr().out
    assert "publication text 2" in output
    assert "sample-widget" not in output
    event.write_text("{", encoding="utf-8")
    assert synthetic_policy.main([]) == 1


def test_explicit_event_and_text_read_errors_are_safe(monkeypatch, tmp_path, capsys):
    monkeypatch.setattr(hygiene, "repository_files", lambda **kwargs: [])
    monkeypatch.setenv("GITHUB_EVENT_NAME", "workflow_dispatch")
    missing = tmp_path / "unavailable.json"
    monkeypatch.setenv("GITHUB_EVENT_PATH", str(missing))
    assert hygiene.main([]) == 0
    assert hygiene.main(["--event", str(missing)]) == 1
    assert hygiene.main(["--text-file", str(missing)]) == 1
    assert str(missing) not in capsys.readouterr().out


@pytest.mark.parametrize("event_name", ["pull_request", "pull_request_target", "push"])
def test_public_event_requires_metadata_file(monkeypatch, event_name):
    monkeypatch.setattr(hygiene, "repository_files", lambda **kwargs: [])
    monkeypatch.setenv("GITHUB_EVENT_NAME", event_name)
    monkeypatch.delenv("GITHUB_EVENT_PATH", raising=False)
    assert hygiene.main([]) == 1


def test_null_body_and_deleted_files_are_supported(tmp_path):
    event = tmp_path / "event.json"
    event.write_text(
        json.dumps({"pull_request": {"title": "fix: validate content", "body": None, "head": {"ref": "fix/content"}}}),
        encoding="utf-8",
    )
    assert hygiene.check_event(event) == []
    assert hygiene.existing_files([event, Path("missing-candidate-file")]) == [event]


@pytest.mark.parametrize("contents", [b"text\0tail", b"\xff"])
def test_prepared_text_rejects_binary_content(monkeypatch, tmp_path, contents):
    monkeypatch.setattr(hygiene, "repository_files", lambda **kwargs: [])
    candidate = tmp_path / "prepared.txt"
    candidate.write_bytes(contents)
    assert hygiene.main(["--text-file", str(candidate)]) == 1


def test_file_patterns_and_exemptions_remain_scoped(synthetic_policy, monkeypatch, tmp_path):
    candidate = tmp_path / "candidate.txt"
    monkeypatch.setattr(synthetic_policy, "CONTENT_PATTERNS", {"sample-pattern": re.compile("sample-marker")})
    candidate.write_text("sample-marker sample-widget", encoding="utf-8")
    assert synthetic_policy.check_content([candidate]) == [
        "sample-pattern: file 1",
        "private-integration-fingerprint: file 1",
    ]
    monkeypatch.setattr(synthetic_policy, "PATTERN_EXEMPT_PATHS", {candidate.as_posix()})
    assert synthetic_policy.check_content([candidate]) == ["private-integration-fingerprint: file 1"]
    candidate.write_bytes(b"\0binary")
    assert synthetic_policy.check_content([candidate]) == []
    candidate.write_bytes(b"\xff")
    assert synthetic_policy.check_content([candidate]) == []
    candidate.unlink()
    assert synthetic_policy.is_binary(candidate)


@pytest.mark.parametrize("include_untracked", [False, True])
def test_file_inventory_keeps_nul_delimited_paths(monkeypatch, include_untracked):
    def git_output(command, **kwargs):
        expected_command_parts = ["git", "ls-files", "-z", "--cached"]
        if include_untracked:
            expected_command_parts.extend(["--others", "--exclude-standard"])
        assert command == expected_command_parts
        return hygiene.subprocess.CompletedProcess(command, 0, b"two\nlines.txt\0normal.txt\0normal.txt\0")

    monkeypatch.setattr(hygiene.subprocess, "run", git_output)
    assert hygiene.repository_files(include_untracked=include_untracked) == [Path("normal.txt"), Path("two\nlines.txt")]


def test_forbidden_paths_use_safe_labels(monkeypatch):
    monkeypatch.setattr(hygiene, "FORBIDDEN_PATH_PARTS", {"sample-only"})
    monkeypatch.setattr(hygiene, "FORBIDDEN_BASENAMES", {"sample.md"})
    assert hygiene.check_paths([Path("sample-only/sample.md"), Path("allowed.md")]) == [
        "forbidden path component: file 1",
        "forbidden instruction file: file 1",
    ]
