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


def test_reads_push_event_subjects(tmp_path):
    module = load_policy_module()
    event_path = tmp_path / "push.json"
    event_path.write_text(
        json.dumps(
            {
                "commits": [
                    {"message": "fix(api): handle timeout\n\nBody text."},
                    {"message": "docs: explain commit style"},
                ]
            }
        ),
        encoding="utf-8",
    )

    assert module.event_subjects(event_path) == [
        "fix(api): handle timeout",
        "docs: explain commit style",
    ]


def test_reads_pull_request_title(tmp_path):
    module = load_policy_module()
    event_path = tmp_path / "pull_request.json"
    event_path.write_text(
        json.dumps({"pull_request": {"title": "ci(commit): add message gate"}}),
        encoding="utf-8",
    )

    assert module.event_subjects(event_path) == ["ci(commit): add message gate"]


def test_main_accepts_direct_message(capsys):
    module = load_policy_module()
    exit_code = module.main(["--message", "fix(api): handle timeout"])

    assert not exit_code
    assert "policy OK" in capsys.readouterr().out


def test_main_rejects_unclear_message(capsys):
    module = load_policy_module()
    exit_code = module.main(["--message", "update stuff"])

    assert exit_code
    assert "policy failed" in capsys.readouterr().out


@pytest.mark.parametrize("option", ["--message", "--last", "--range"])
def test_full_messages_are_checked_before_style_output(monkeypatch, capsys, option):
    module = load_policy_module()
    hygiene = importlib.import_module("ci.public_hygiene")
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
    hygiene = importlib.import_module("ci.public_hygiene")
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
    assert module.git_subjects(["base..HEAD"]) == ["fix: validate content"]


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


def test_legacy_subject_helpers_keep_their_contract(monkeypatch):
    module = load_policy_module()
    monkeypatch.setattr(module, "event_subjects", lambda path: ["docs: explain policy"])
    monkeypatch.setattr(module, "git_subjects", lambda arguments: ["fix: preserve full messages"])
    args = module.parse_args(["--event", "event.json", "--last", "1", "--range", "base..HEAD", "--message", "test: cover input"])
    assert len(module.cli_subjects(args)) == 4
    assert module.cli_subjects(module.parse_args([])) == []
    assert module.push_subjects({"head_commit": {"message": "fix: validate content\n\nBody."}}) == ["fix: validate content"]
    assert module.validate_subject("fix: " + "x" * 100) == ["subject is longer than 100 characters"]


def test_empty_selection_is_reported(capsys):
    module = load_policy_module()
    assert module.main([]) == 2
    assert "No commit subjects" in capsys.readouterr().err
