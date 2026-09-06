"""Source file discovery and per-file readability checks."""

from __future__ import annotations

import ast
import hashlib
import re
import subprocess
from pathlib import Path
from typing import Any

from .config import (DEFAULT_COMMENT_NOISE_PATTERNS, compile_suppression_patterns, is_matching_path_pattern,
                     readability_options, threshold)
from .function_visitor import FunctionVisitor
from .function_names import confusable_function_name_issues
from .model import DEFAULT_ISSUE_CATEGORIES, Issue


def collect_issues(
    repo_root: Path,
    config: dict[str, Any],
    base_revision: str | None = None,
) -> dict[str, list[Issue]]:
    """Collect readability findings grouped by rule category."""
    patterns = compile_suppression_patterns(config)
    issues_by_category: dict[str, list[Issue]] = {category: [] for category in DEFAULT_ISSUE_CATEGORIES}
    source_files = _iter_source_files(repo_root, config)
    length_config_by_field = dict(config, source_roots=readability_options(config).get("file_length_roots", config.get("source_roots", [])))
    length_files = _iter_source_files(repo_root, length_config_by_field)
    for path in sorted(set(length_files) - set(source_files)):
        issues_by_category["long_files"].extend(
            _file_size_issues(path.relative_to(repo_root).as_posix(), path, config)
        )
    for path in source_files:
        for issue in _analyze_file(repo_root, path, config):
            issues_by_category[issue.category].append(issue)
        issues_by_category["inline_suppressions"].extend(_find_inline_suppressions(repo_root, path, patterns))
        if path.suffix == ".py":
            issues_by_category["comment_noise"].extend(_find_comment_noise(repo_root, path, config))
    python_paths = [path for path in source_files if path.suffix == ".py"]
    issues_by_category["confusable_function_names"].extend(
        confusable_function_name_issues(repo_root, python_paths)
    )
    if base_revision:
        issues_by_category["huge_file_growth"].extend(
            _huge_file_growth_issues(repo_root, length_files, config, base_revision)
        )
    return {category: sorted(issues, key=lambda issue: issue.identifier) for category, issues in issues_by_category.items()}


def _iter_source_files(repo_root: Path, config: dict[str, Any]) -> list[Path]:
    roots = config.get("source_roots", [])
    exclude_globs = config.get("exclude_globs", [])
    include_suffixes = tuple(config.get("include_suffixes", [".py"]))
    files: list[Path] = []
    for source_root in roots:
        root_path = repo_root / source_root
        candidates = _candidate_files(root_path)
        for path in candidates:
            relative = path.relative_to(repo_root).as_posix()
            if not path.name.endswith(include_suffixes):
                continue
            if is_matching_path_pattern(relative, exclude_globs):
                continue
            files.append(path)
    return sorted(set(files))


def _candidate_files(root_path: Path) -> list[Path]:
    if root_path.is_file():
        return [root_path]
    if root_path.is_dir():
        return [path for path in root_path.rglob("*") if path.is_file()]
    return []


def _line_count(path: Path) -> int:
    with path.open("r", encoding="utf-8") as handle:
        return sum(1 for _ in handle)


def _suppression_fingerprint(path: str, pattern_name: str, line: str) -> str:
    normalized = " ".join(line.strip().split())
    digest = hashlib.sha1(f"{path}:{pattern_name}:{normalized}".encode("utf-8")).hexdigest()[:12]
    return f"inline_suppression:{path}:{pattern_name}:{digest}"


def _find_inline_suppressions(
    repo_root: Path,
    path: Path,
    patterns: list[tuple[str, re.Pattern[str]]],
) -> list[Issue]:
    relative = path.relative_to(repo_root).as_posix()
    issues: list[Issue] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            for pattern_name, pattern in patterns:
                if pattern.search(line):
                    issues.append(
                        Issue(
                            "inline_suppressions",
                            _suppression_fingerprint(relative, pattern_name, line),
                            relative,
                            {
                                "line": line_number,
                                "pattern": pattern_name,
                                "text": line.strip(),
                            },
                        )
                    )
    return issues


def _find_comment_noise(repo_root: Path, path: Path, config: dict[str, Any]) -> list[Issue]:
    relative = path.relative_to(repo_root).as_posix()
    patterns = [
        re.compile(pattern, re.IGNORECASE)
        for pattern in readability_options(config).get("comment_noise_patterns", DEFAULT_COMMENT_NOISE_PATTERNS)
    ]
    issues: list[Issue] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            issue = _comment_noise_issue(relative, line_number, line, patterns)
            if issue:
                issues.append(issue)
    return issues


def _comment_noise_issue(
    relative: str,
    line_number: int,
    line: str,
    patterns: list[re.Pattern[str]],
) -> Issue | None:
    stripped = line.strip()
    if not stripped.startswith("#"):
        return None
    comment = stripped.lstrip("#").strip()
    if not comment or comment.startswith(("!", "-", "Licensed", "Copyright")):
        return None
    if not any(pattern.search(comment) for pattern in patterns):
        return None
    digest = hashlib.sha1(f"{relative}:{line_number}:{comment}".encode("utf-8")).hexdigest()[:12]
    return Issue(
        "comment_noise",
        f"comment_noise:{relative}:{digest}",
        relative,
        {"line": line_number, "text": comment},
    )


def _analyze_file(repo_root: Path, path: Path, config: dict[str, Any]) -> list[Issue]:
    relative = path.relative_to(repo_root).as_posix()
    issues = _file_size_issues(relative, path, config) if _is_file_length_path(relative, config) else []
    if path.suffix != ".py":
        return issues
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"))
    except SyntaxError as exc:
        return [
            Issue(
                "syntax_errors",
                f"syntax_error:{relative}:{exc.lineno}:{exc.offset}",
                relative,
                {"line": exc.lineno, "offset": exc.offset, "message": exc.msg},
            )
        ]
    visitor = FunctionVisitor(repo_root, path, config)
    visitor.visit(tree)
    issues.extend(visitor.issues)
    return issues


def _huge_file_git_issue(name: str, path: str = ".") -> Issue:
    return Issue(
        "huge_file_growth",
        f"huge_file_growth:git:{name}:{path}",
        path,
        {"line": 1, "name": name},
    )


def _renamed_base_path_by_current(
    repo_root: Path,
    base_revision: str,
) -> tuple[dict[str, str], Issue | None]:
    completed = subprocess.run(
        [
            "git",
            "diff",
            "--name-status",
            "-z",
            "--find-renames=1%",
            f"{base_revision}..HEAD",
            "--",
        ],
        cwd=repo_root,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    if completed.returncode:
        return {}, _huge_file_git_issue("base diff unavailable")
    fields = completed.stdout.split(b"\0")
    renamed_base_path_by_current: dict[str, str] = {}
    field_index = 0
    while field_index < len(fields) and fields[field_index]:
        status = fields[field_index].decode("utf-8")
        field_index += 1
        if "\t" in status:
            status, base_path = status.split("\t", 1)
        else:
            base_path = fields[field_index].decode("utf-8")
            field_index += 1
        if not status.startswith(("R", "C")):
            continue
        current_path = fields[field_index].decode("utf-8")
        field_index += 1
        if status.startswith("R"):
            renamed_base_path_by_current[current_path] = base_path
    return renamed_base_path_by_current, None


def _base_file_lines(
    repo_root: Path,
    base_revision: str,
    relative: str,
) -> tuple[int | None, Issue | None]:
    listing = subprocess.run(
        ["git", "ls-tree", "-z", "--name-only", base_revision, "--", relative],
        cwd=repo_root,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    if listing.returncode:
        return None, _huge_file_git_issue("base tree unavailable", relative)
    if not listing.stdout:
        return None, None
    completed = subprocess.run(
        ["git", "show", f"{base_revision}:{relative}"],
        cwd=repo_root,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    if completed.returncode:
        return None, _huge_file_git_issue("base file unavailable", relative)
    return len(completed.stdout.splitlines()), None


def _huge_file_growth_issues(
    repo_root: Path,
    source_files: list[Path],
    config: dict[str, Any],
    base_revision: str,
) -> list[Issue]:
    threshold_lines = threshold(config, "huge_file_lines", 5000)
    verify_base = subprocess.run(
        ["git", "rev-parse", "--verify", f"{base_revision}^{{commit}}"],
        cwd=repo_root,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    if verify_base.returncode:
        return [_huge_file_git_issue("base revision unavailable")]
    renamed_base_path_by_current, rename_error = _renamed_base_path_by_current(
        repo_root, base_revision
    )
    if rename_error:
        return [rename_error]
    issues: list[Issue] = []
    for path in source_files:
        relative = path.relative_to(repo_root).as_posix()
        current_lines = _line_count(path)
        if current_lines <= threshold_lines or not _is_file_length_path(relative, config):
            continue
        base_relative = renamed_base_path_by_current.get(relative, relative)
        base_lines, lookup_error = _base_file_lines(
            repo_root,
            base_revision,
            base_relative,
        )
        if lookup_error:
            issues.append(lookup_error)
            continue
        if base_lines is None:
            continue
        if base_lines > threshold_lines and current_lines > base_lines:
            issues.append(
                Issue(
                    "huge_file_growth",
                    f"huge_file_growth:{relative}",
                    relative,
                    {"line": 1, "lines": current_lines, "limit": base_lines},
                )
            )
    return issues


def _is_file_length_path(relative: str, config: dict[str, Any]) -> bool:
    roots = readability_options(config).get("file_length_roots", config.get("source_roots", []))
    return any(relative == root or relative.startswith(f"{root.rstrip('/')}/") for root in roots)


def _file_size_issues(relative: str, path: Path, config: dict[str, Any]) -> list[Issue]:
    file_lines = _line_count(path)
    max_file_lines = threshold(config, "max_file_lines", 1500)
    if file_lines <= max_file_lines:
        return []
    return [
        Issue(
            "long_files",
            f"long_file:{relative}",
            relative,
            {"lines": file_lines, "limit": max_file_lines},
        )
    ]
