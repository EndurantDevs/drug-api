#!/usr/bin/env python3
"""Validate commit subjects and pull request titles for readability."""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.ci.public_hygiene import check_text, event_texts

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
        problems.append("unsupported commit type")
    if vague_summary_key(summary) in VAGUE_SUMMARIES:
        problems.append("summary is too vague")
    return problems


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


def print_problems(problems_by_label: list[tuple[str, list[str]]]) -> None:
    """Print validation failures in a CI-friendly format."""
    print("Commit message policy failed:")
    for label, problems in problems_by_label:
        print(f"  {label}")
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
        labeled_subjects = [
            (label, first_line(text))
            for label, text in public_text_pairs
            if (label == "PR title" or label.startswith(("commit message ", "push commit ")))
            and first_line(text)
        ]
    except (OSError, ValueError, subprocess.CalledProcessError):
        print("Cannot validate commit messages or event metadata.", file=sys.stderr)
        return 2
    if not labeled_subjects:
        print("No commit subjects found to validate.", file=sys.stderr)
        return 2

    subject_problem_pairs = [
        (label, problems)
        for label, subject in labeled_subjects
        if (problems := validate_subject(subject))
    ]
    if subject_problem_pairs:
        print_problems(subject_problem_pairs)
        return 1
    print(f"Commit message policy OK ({len(labeled_subjects)} subject(s)).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
