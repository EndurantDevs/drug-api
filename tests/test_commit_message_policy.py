import hashlib
import importlib.util
import json
from pathlib import Path

import pytest


def load_policy_module():
    script_path = Path(__file__).resolve().parents[1] / "scripts" / "check_commit_messages.py"
    spec = importlib.util.spec_from_file_location("check_commit_messages", script_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize(
    "subject",
    [
        "fix(api): handle upstream timeout",
        "feat(ptg)!: require explicit source routing",
        "docs: explain commit message style",
        "Merge pull request #123 from EndurantDevs/example",
        "Revert \"fix(api): handle upstream timeout\"",
        "Bump actions/checkout from 4 to 5",
    ],
)
def test_accepts_clear_commit_subjects(subject):
    module = load_policy_module()
    assert module.validate_subject(subject) == []


@pytest.mark.parametrize(
    "subject",
    [
        "",
        "fix",
        "fix: fix",
        "update stuff",
        "feature(api): add route",
        "fix(API): handle timeout",
        "fix(api): handle timeout.",
        "fix(api) handle timeout",
    ],
)
def test_rejects_unclear_commit_subjects(subject):
    module = load_policy_module()
    assert module.validate_subject(subject)


def test_main_accepts_direct_message(capsys):
    module = load_policy_module()
    exit_code = module.main(["--message", "fix(api): handle timeout"])

    assert not exit_code
    assert "policy OK" in capsys.readouterr().out


def test_main_rejects_unclear_message(capsys):
    module = load_policy_module()
    exit_code = module.main(["--message", "update stuff"])

    assert exit_code
    output = capsys.readouterr().out
    assert "policy failed" in output
    assert "commit message 1" in output
    assert "update stuff" not in output


def test_event_style_errors_report_trusted_label(tmp_path, capsys):
    module = load_policy_module()
    event_path = tmp_path / "pull_request.json"
    event_path.write_text(json.dumps({"pull_request": {
        "title": "update stuff", "body": "Public details.", "head": {"ref": "fix/public"},
    }}), encoding="utf-8")

    assert module.main(["--event", str(event_path)]) == 1
    output = capsys.readouterr().out
    assert "PR title" in output
    assert "update stuff" not in output


def test_unsupported_type_diagnostic_is_redacted(capsys):
    module = load_policy_module()
    rejected = "sampleprivateclient"

    assert module.main(["--message", f"{rejected}: preserve behavior"]) == 1
    output = capsys.readouterr().out
    assert "unsupported commit type" in output
    assert rejected not in output


@pytest.mark.parametrize("option", ["--message", "--last", "--range"])
def test_full_messages_are_checked_before_style_output(monkeypatch, capsys, option):
    module = load_policy_module()
    hygiene = importlib.import_module("scripts.ci.public_hygiene")
    monkeypatch.setattr(hygiene, "PRIVATE_INTEGRATION_FINGERPRINTS", {hashlib.sha256(b"samplewidget").hexdigest()})
    message = "unclear sample-widget\n\nAdditional detail."
    monkeypatch.setattr(module, "git_messages", lambda arguments: [message])
    argument = {"--message": message, "--last": "1", "--range": "base..HEAD"}[option]
    assert module.main([option, argument]) == 1
    output = capsys.readouterr().out
    assert "private-integration-fingerprint: commit message 1" in output
    assert "sample-widget" not in output
    assert "Expected:" not in output


def test_event_commit_body_is_checked(monkeypatch, capsys, tmp_path):
    module = load_policy_module()
    hygiene = importlib.import_module("scripts.ci.public_hygiene")
    monkeypatch.setattr(hygiene, "PRIVATE_INTEGRATION_FINGERPRINTS", {hashlib.sha256(b"samplewidget").hexdigest()})
    event = tmp_path / "event.json"
    event.write_text(
        json.dumps({"ref": "refs/heads/dev", "commits": [{"message": "fix: validate content\n\nsample-widget"}]}),
        encoding="utf-8",
    )
    assert module.main(["--event", str(event)]) == 1
    assert "sample-widget" not in capsys.readouterr().out


def test_git_messages_preserves_commit_bodies(monkeypatch):
    module = load_policy_module()
    message = "fix: validate content\n\nAdditional detail."

    def git_output(command, **kwargs):
        assert command == ["git", "log", "--format=%B%x00", "base..HEAD"]
        return module.subprocess.CompletedProcess(command, 0, message + "\n\0\n")

    monkeypatch.setattr(module.subprocess, "run", git_output)
    assert module.git_messages(["base..HEAD"]) == [message]


def test_malformed_event_never_echoes_input(capsys, tmp_path):
    module = load_policy_module()
    event = tmp_path / "untrusted-title.json"
    event.write_text('{"pull_request": {"title": "untrusted-title"}}', encoding="utf-8")
    assert module.main(["--event", str(event)]) == 2
    output = capsys.readouterr()
    assert "untrusted-title" not in output.out + output.err


@pytest.mark.parametrize("selector", ["--last=0", "--last=-1", "--range=", "--range=--format=%s"])
def test_invalid_commit_selection_fails_before_read(monkeypatch, capsys, selector):
    module = load_policy_module()

    def unexpected_read(arguments):
        pytest.fail("invalid selection reached git")

    monkeypatch.setattr(module, "git_messages", unexpected_read)
    assert module.main([selector]) == 2
    output = capsys.readouterr()
    assert selector not in output.out + output.err


def test_empty_selection_is_reported(capsys):
    module = load_policy_module()
    assert module.main([]) == 2
    assert "No commit subjects" in capsys.readouterr().err
