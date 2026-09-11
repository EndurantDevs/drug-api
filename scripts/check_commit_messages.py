#!/usr/bin/env python3
"""Validate commit subjects and pull request titles for readability."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))
from ci.public_hygiene import check_text, event_texts

ALLOWED_TYPES = {
    "build",
    "chore",
    "ci",
    "deploy",
    "docs",
    "feat",
    "fix",
    "ops",
    "perf",
    "refactor",
    "revert",
    "test",
}
AUTOMATION_PREFIXES = (
    "Bump ",
    "Merge ",
    "Revert ",
)
MAX_SUBJECT_LENGTH = 100
VAGUE_SUMMARIES = {
    "change",
    "changes",
    "cleanup",
    "fix",
    "fixes",
    "misc",
    "stuff",
    "update",
    "updates",
    "wip",
    "work",
}
SUBJECT_PATTERN = re.compile(
    r"^(?P<type>[a-z]+)"
    r"(?:\((?P<scope>[a-z0-9][a-z0-9-]*(?:/[a-z0-9][a-z0-9-]*)*)\))?"
    r"(?P<breaking>!)?: (?P<summary>.+)$"
)


def first_line(message: str) -> str:
    """Return the first non-empty line from a commit message."""
    return message.strip().splitlines()[0].strip() if message.strip() else ""


def is_automation_subject(subject: str) -> bool:
    """Return whether GitHub or Dependabot generated the subject."""
    return subject.startswith(AUTOMATION_PREFIXES)


def vague_summary_key(summary: str) -> str:
    """Normalize a summary so low-signal wording can be detected."""
    return summary.strip().lower().rstrip(".!?")


def validate_subject(subject: str) -> list[str]:
    """Return readability problems for one commit subject."""
    problems: list[str] = []
    if not subject:
        return ["subject is empty"]
    if is_automation_subject(subject):
        return problems
    if len(subject) > MAX_SUBJECT_LENGTH:
        problems.append(f"subject is longer than {MAX_SUBJECT_LENGTH} characters")
    if subject.endswith("."):
        problems.append("subject must not end with a period")

    match = SUBJECT_PATTERN.match(subject)
    if not match:
        problems.append("use 'type(scope): imperative summary' or 'type: imperative summary'")
        return problems

    commit_type = match.group("type")
    summary = match.group("summary")
    if commit_type not in ALLOWED_TYPES:
        problems.append(f"unsupported type '{commit_type}'")
    if vague_summary_key(summary) in VAGUE_SUMMARIES:
        problems.append("summary is too vague")
    return problems


def push_subjects(payload: dict[str, Any]) -> list[str]:
    """Return commit subjects from a GitHub push event payload."""
    raw_commits = payload.get("commits")
    commit_list = raw_commits if isinstance(raw_commits, list) else []
    subject_list = [
        first_line(str(commit.get("message", "")))
        for commit in commit_list
        if isinstance(commit, dict)
    ]
    if not subject_list and isinstance(payload.get("head_commit"), dict):
        subject_list.append(first_line(str(payload["head_commit"].get("message", ""))))
    return [subject for subject in subject_list if subject]


def pull_request_subjects(payload: dict[str, Any]) -> list[str]:
    """Return the title from a GitHub pull request event payload."""
    pull_request = payload.get("pull_request")
    if not isinstance(pull_request, dict):
        return []
    title = str(pull_request.get("title", "")).strip()
    return [title] if title else []


def event_subjects(event_path: Path) -> list[str]:
    """Return subjects to validate from a GitHub event JSON file."""
    event_payload = json.loads(event_path.read_text(encoding="utf-8"))
    subject_list = pull_request_subjects(event_payload)
    if subject_list:
        return subject_list
    return push_subjects(event_payload)


def git_subjects(arguments: list[str]) -> list[str]:
    """Return subjects from git log for the given revision arguments."""
    return [first_line(message) for message in git_messages(arguments) if first_line(message)]


def git_messages(arguments: list[str]) -> list[str]:
    """Return complete commit messages, preserving bodies for publication checks."""
    git_command_parts = ["git", "log", "--format=%B%x00", *arguments]
    completed = subprocess.run(git_command_parts, check=True, text=True, capture_output=True)
    return [message.strip() for message in completed.stdout.split("\0") if message.strip()]


def parse_args(argv: list[str]) -> argparse.Namespace:
    """Parse command line options."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--event", type=Path, help="GitHub event JSON payload")
    parser.add_argument("--message", action="append", default=[], help="Commit subject or full message")
    parser.add_argument("--last", type=int, help="Validate the last N git commits")
    parser.add_argument("--range", dest="commit_range", help="Validate a git revision range")
    return parser.parse_args(argv)


def cli_subjects(args: argparse.Namespace) -> list[str]:
    """Return all subjects requested by command line options."""
    subject_list = [first_line(message) for message in args.message]
    if args.event:
        subject_list.extend(event_subjects(args.event))
    if args.last:
        subject_list.extend(git_subjects([f"-n{args.last}"]))
    if args.commit_range:
        subject_list.extend(git_subjects([args.commit_range]))
    return [subject for subject in subject_list if subject]


def cli_public_texts(args: argparse.Namespace) -> list[tuple[str, str]]:
    """Collect complete messages and event metadata before any diagnostics."""
    if args.last is not None and args.last <= 0:
        raise ValueError("Requested commit count must be positive.")
    if args.commit_range is not None and (not args.commit_range or args.commit_range.startswith("-")):
        raise ValueError("Requested revision range must name commits.")
    message_list = list(args.message)
    if args.last:
        message_list.extend(git_messages([f"-n{args.last}"]))
    if args.commit_range:
        message_list.extend(git_messages([args.commit_range]))
    text_list = [(f"commit message {index}", message) for index, message in enumerate(message_list, 1)]
    if args.event:
        text_list.extend(event_texts(args.event))
    return text_list


def print_problems(problems_by_subject: list[tuple[str, list[str]]]) -> None:
    """Print validation failures in a CI-friendly format."""
    print("Commit message policy failed:")
    for subject, problems in problems_by_subject:
        print(f"  {subject}")
        for problem in problems:
            print(f"    - {problem}")
    print("\nExpected: type(scope): imperative summary")
    print("Allowed types: " + ", ".join(sorted(ALLOWED_TYPES)))


def main(argv: list[str] | None = None) -> int:
    """Run the commit message policy check."""
    args = parse_args(argv if argv is not None else sys.argv[1:])
    return validate_message_inputs(args)


def validate_message_inputs(args: argparse.Namespace) -> int:
    """Validate full publication content before rendering any subject errors."""
    try:
        public_text_pairs = cli_public_texts(args)
        public_errors = [error for label, text in public_text_pairs for error in check_text(text, label)]
        if public_errors:
            print("Public hygiene check failed:")
            for error in public_errors:
                print(f"- {error}")
            return 1
        subject_list = [
            first_line(text)
            for label, text in public_text_pairs
            if (label == "PR title" or label.startswith(("commit message ", "push commit ")))
            and first_line(text)
        ]
    except (OSError, ValueError, subprocess.CalledProcessError):
        print("Cannot validate commit messages or event metadata.", file=sys.stderr)
        return 2
    if not subject_list:
        print("No commit subjects found to validate.", file=sys.stderr)
        return 2

    subject_problem_pairs = [
        (subject, problems)
        for subject in subject_list
        if (problems := validate_subject(subject))
    ]
    if subject_problem_pairs:
        print_problems(subject_problem_pairs)
        return 1
    print(f"Commit message policy OK ({len(subject_list)} subject(s)).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
