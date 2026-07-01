#!/usr/bin/env python3
"""Measure readability debt and fail when new debt is introduced."""

from __future__ import annotations

import argparse
import ast
import builtins
import fnmatch
import hashlib
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

DEFAULT_CONFIG = "readability-budget.json"
DEFAULT_BASELINE = "readability-baseline.json"
DEFAULT_AMBIGUOUS_FUNCTION_NAMES = {
    "build",
    "callback",
    "do",
    "execute",
    "get",
    "get_all",
    "handle",
    "helper",
    "load",
    "main",
    "parse",
    "process",
    "process_data",
    "run",
    "save",
    "shutdown",
    "start",
}
DEFAULT_AMBIGUOUS_VARIABLE_NAMES = {
    "data",
    "item",
    "items",
    "obj",
    "payload",
    "record",
    "records",
    "res",
    "result",
    "results",
    "row",
    "rows",
    "source",
    "sources",
    "target",
    "targets",
    "tmp",
    "value",
    "values",
}
DEFAULT_BOOLEAN_PREFIXES = (
    "can_",
    "has_",
    "include_",
    "includes_",
    "is_",
    "needs_",
    "should_",
    "supports_",
    "use_",
    "uses_",
)
DEFAULT_ALLOWED_SHORT_NAMES = {"_", "i", "j", "k", "n", "x", "y", "z"}
DEFAULT_ALWAYS_BAD_SHORT_NAMES = {"l", "O"}
DEFAULT_COLLECTION_SINGULAR_EXCEPTIONS = {
    "data",
    "metadata",
    "payload",
}
DEFAULT_DICT_NAME_MARKERS = (
    "by_",
    "_by_",
    "_dict",
    "_index",
    "_lookup",
    "_map",
    "_registry",
)
DEFAULT_COMMENT_NOISE_PATTERNS = (
    r"^(?:increment|decrement|return|returns|loop|iterate|assign|set|get|create|call|print)\b",
    r"^(?:initialize|initialise) (?:variable|counter|list|dict|dictionary|set)\b",
)
DEFAULT_ISSUE_CATEGORIES = (
    "ambiguous_function_names",
    "ambiguous_variable_names",
    "boolean_name_mismatch",
    "builtin_shadowing",
    "class_name_shape",
    "collection_name_mismatch",
    "comment_noise",
    "deep_nesting",
    "function_name_shape",
    "global_state_usage",
    "inline_suppressions",
    "long_files",
    "long_functions",
    "missing_contract_docstrings",
    "pass_placeholders",
    "single_letter_names",
    "syntax_errors",
    "too_many_locals",
    "too_many_parameters",
)
NESTING_NODES = (
    ast.If,
    ast.For,
    ast.AsyncFor,
    ast.While,
    ast.With,
    ast.AsyncWith,
    ast.Try,
    ast.Match,
)
SCOPE_NODES = (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Lambda)
BUILTIN_NAMES = set(dir(builtins))


@dataclass(frozen=True)
class Issue:
    category: str
    identifier: str
    path: str
    detail: dict[str, Any]

    def to_json(self) -> dict[str, Any]:
        """Return the stable JSON shape used by baselines and CLI output."""
        payload = {"id": self.identifier, "path": self.path}
        payload.update(self.detail)
        return payload


@dataclass(frozen=True)
class LocalNameRules:
    ambiguous_names: set[str]
    allowed_short_names: set[str]
    always_bad_short_names: set[str]
    min_vague_scope_lines: int
    max_short_scope_lines: int


def _load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _compile_suppression_patterns(config: dict[str, Any]) -> list[tuple[str, re.Pattern[str]]]:
    patterns = []
    for pattern in config.get("inline_suppression_patterns", []):
        patterns.append((pattern["name"], re.compile(pattern["pattern"])))
    return patterns


def _is_matching_path_pattern(path: str, patterns: Iterable[str]) -> bool:
    return any(fnmatch.fnmatch(path, pattern) for pattern in patterns)


def _readability_options(config: dict[str, Any]) -> dict[str, Any]:
    return config.get("readability", {})


def _threshold(config: dict[str, Any], name: str, default: int) -> int:
    value = config.get("thresholds", {}).get(name)
    if value is None:
        value = _readability_options(config).get(name, default)
    return int(value)


def _name_list(config: dict[str, Any], name: str, default: Iterable[str]) -> set[str]:
    configured = _readability_options(config).get(name)
    if configured is None:
        return set(default)
    return set(configured)


def _name_prefixes(config: dict[str, Any], name: str, default: Iterable[str]) -> tuple[str, ...]:
    configured = _readability_options(config).get(name)
    if configured is None:
        return tuple(default)
    return tuple(configured)


def _split_name_tokens(name: str) -> list[str]:
    cleaned = name.strip("_")
    if not cleaned:
        return []
    parts: list[str] = []
    for token in cleaned.split("_"):
        token_parts = re.findall(r"[A-Z]?[a-z]+|[A-Z]+(?=[A-Z]|$)|[0-9]+", token)
        parts.extend(part.lower() for part in token_parts if part)
    return parts or [cleaned.lower()]


def _has_boolean_prefix(name: str, prefixes: Iterable[str]) -> bool:
    normalized = name.strip("_")
    return any(normalized.startswith(prefix) for prefix in prefixes)


def _is_boolean_method_name(name: str) -> bool:
    normalized = name.strip("_")
    known_predicates = {
        "isalnum",
        "isalpha",
        "isascii",
        "isdecimal",
        "isdigit",
        "isidentifier",
        "islower",
        "isnumeric",
        "isspace",
        "istitle",
        "isupper",
        "empty",
        "full",
        "in_transaction",
        "startswith",
        "endswith",
    }
    return normalized in known_predicates or normalized.startswith(("is_", "has_"))


def _is_bool_expression(node: ast.AST) -> bool:
    if isinstance(node, ast.Constant) and isinstance(node.value, bool):
        return True
    if isinstance(node, ast.Compare):
        return True
    if isinstance(node, ast.BoolOp):
        return all(_is_bool_expression(value) for value in node.values)
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.Not):
        return True
    if isinstance(node, ast.Call):
        if isinstance(node.func, ast.Attribute):
            return _is_boolean_method_name(node.func.attr)
        if isinstance(node.func, ast.Name):
            return _has_boolean_prefix(node.func.id, DEFAULT_BOOLEAN_PREFIXES)
    return False


def _is_boolean_or_none_literal(node: ast.AST) -> bool:
    return isinstance(node, ast.Constant) and (isinstance(node.value, bool) or node.value is None)


def _is_ellipsis_expr(node: ast.AST) -> bool:
    return isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant) and node.value.value is Ellipsis


def _collection_expression_kind(node: ast.AST) -> str | None:
    if isinstance(node, (ast.List, ast.ListComp, ast.GeneratorExp)):
        return "list"
    if isinstance(node, (ast.Set, ast.SetComp)):
        return "set"
    if isinstance(node, (ast.Dict, ast.DictComp)):
        return "dict"
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
        if node.func.id in {"list", "tuple"}:
            return "list"
        if node.func.id == "set":
            return "set"
        if node.func.id == "dict":
            return "dict"
    return None


def _iter_local_nodes(node: ast.FunctionDef | ast.AsyncFunctionDef) -> Iterable[ast.AST]:
    for child in node.body:
        yield from _iter_without_nested_scopes(child)


def _iter_without_nested_scopes(node: ast.AST) -> Iterable[ast.AST]:
    yield node
    for child in ast.iter_child_nodes(node):
        if isinstance(child, SCOPE_NODES):
            continue
        yield from _iter_without_nested_scopes(child)


def _target_names(node: ast.AST) -> list[str]:
    if isinstance(node, ast.Name):
        return [node.id]
    if isinstance(node, (ast.Tuple, ast.List)):
        names: list[str] = []
        for element in node.elts:
            names.extend(_target_names(element))
        return names
    return []


def _iter_source_files(repo_root: Path, config: dict[str, Any]) -> list[Path]:
    roots = config.get("source_roots", [])
    exclude_globs = config.get("exclude_globs", [])
    include_suffixes = tuple(config.get("include_suffixes", [".py"]))
    files: list[Path] = []
    for source_root in roots:
        root_path = repo_root / source_root
        if root_path.is_file():
            candidates = [root_path]
        elif root_path.is_dir():
            candidates = [path for path in root_path.rglob("*") if path.is_file()]
        else:
            continue
        for path in candidates:
            relative = path.relative_to(repo_root).as_posix()
            if not path.name.endswith(include_suffixes):
                continue
            if _is_matching_path_pattern(relative, exclude_globs):
                continue
            files.append(path)
    return sorted(set(files))


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
        for pattern in _readability_options(config).get("comment_noise_patterns", DEFAULT_COMMENT_NOISE_PATTERNS)
    ]
    issues: list[Issue] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            stripped = line.strip()
            if not stripped.startswith("#"):
                continue
            comment = stripped.lstrip("#").strip()
            if not comment or comment.startswith(("!", "-", "Licensed", "Copyright")):
                continue
            if any(pattern.search(comment) for pattern in patterns):
                digest = hashlib.sha1(f"{relative}:{line_number}:{comment}".encode("utf-8")).hexdigest()[:12]
                issues.append(
                    Issue(
                        "comment_noise",
                        f"comment_noise:{relative}:{digest}",
                        relative,
                        {"line": line_number, "text": comment},
                    )
                )
    return issues


def _is_test_function(relative: str, name: str) -> bool:
    return relative.startswith("tests/") and name.startswith("test_")


def _max_nesting_depth(node: ast.AST, depth: int = 0) -> int:
    if isinstance(node, SCOPE_NODES):
        return depth
    current_depth = depth + 1 if isinstance(node, NESTING_NODES) else depth
    max_depth = current_depth
    for child in ast.iter_child_nodes(node):
        max_depth = max(max_depth, _max_nesting_depth(child, current_depth))
    return max_depth


class FunctionVisitor(ast.NodeVisitor):
    def __init__(self, repo_root: Path, path: Path, config: dict[str, Any]) -> None:
        self.repo_root = repo_root
        self.path = path
        self.config = config
        self.thresholds = config["thresholds"]
        self.issues: list[Issue] = []
        self.scope: list[str] = []

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        """Check class-level naming rules before walking nested definitions."""
        self._check_class_name(node)
        self.scope.append(node.name)
        self.generic_visit(node)
        self.scope.pop()

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        """Check synchronous functions with the shared function rules."""
        self._visit_function(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        """Check asynchronous functions with the shared function rules."""
        self._visit_function(node)

    def _visit_function(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        relative = self.path.relative_to(self.repo_root).as_posix()
        qualified_name = ".".join([*self.scope, node.name])
        function_lines = node.end_lineno - node.lineno + 1 if node.end_lineno else 0
        max_function_lines = _threshold(self.config, "max_function_lines", 60)
        if function_lines > max_function_lines:
            self.issues.append(
                Issue(
                    "long_functions",
                    f"long_function:{relative}:{qualified_name}",
                    relative,
                    {
                        "function": qualified_name,
                        "line": node.lineno,
                        "lines": function_lines,
                        "limit": max_function_lines,
                    },
                )
            )
        max_nesting_depth = max((_max_nesting_depth(child) for child in node.body), default=0)
        max_allowed_depth = _threshold(self.config, "max_nesting_depth", 4)
        if max_nesting_depth > max_allowed_depth:
            self.issues.append(
                Issue(
                    "deep_nesting",
                    f"deep_nesting:{relative}:{qualified_name}",
                    relative,
                    {
                        "function": qualified_name,
                        "line": node.lineno,
                        "depth": max_nesting_depth,
                        "limit": max_allowed_depth,
                    },
                )
            )
        self._check_function_name(node, qualified_name, function_lines)
        self._check_function_contract(node, qualified_name, function_lines)
        self._check_function_locals(node, qualified_name, function_lines)
        self.scope.append(node.name)
        self.generic_visit(node)
        self.scope.pop()

    def _check_class_name(self, node: ast.ClassDef) -> None:
        relative = self.path.relative_to(self.repo_root).as_posix()
        class_name = ".".join([*self.scope, node.name])
        tokens = _split_name_tokens(node.name)
        max_tokens = _threshold(self.config, "max_class_name_tokens", 6)
        generic_names = _name_list(
            self.config,
            "ambiguous_class_names",
            {"Base", "Data", "Handler", "Helper", "Manager", "Processor", "Service", "Wrapper"},
        )
        if node.name in generic_names:
            self.issues.append(
                Issue(
                    "class_name_shape",
                    f"class_name_shape:{relative}:{class_name}:generic",
                    relative,
                    {"class": class_name, "line": node.lineno, "name": node.name, "reason": "generic"},
                )
            )
        if len(tokens) > max_tokens:
            self.issues.append(
                Issue(
                    "class_name_shape",
                    f"class_name_shape:{relative}:{class_name}:too_many_tokens",
                    relative,
                    {
                        "class": class_name,
                        "line": node.lineno,
                        "name": node.name,
                        "tokens": len(tokens),
                        "limit": max_tokens,
                        "reason": "too_many_tokens",
                    },
                )
            )

    def _check_function_name(
        self,
        node: ast.FunctionDef | ast.AsyncFunctionDef,
        qualified_name: str,
        function_lines: int,
    ) -> None:
        """Report vague names, overlong names, and boolean predicate mismatches."""
        relative = self.path.relative_to(self.repo_root).as_posix()
        self._check_ambiguous_function_name(node, qualified_name, function_lines, relative)
        self._check_function_name_length(node, qualified_name, relative)
        self._check_boolean_function_name(node, qualified_name, relative)

    def _check_ambiguous_function_name(
        self,
        node: ast.FunctionDef | ast.AsyncFunctionDef,
        qualified_name: str,
        function_lines: int,
        relative: str,
    ) -> None:
        name = node.name.strip("_")
        ambiguous_names = _name_list(
            self.config,
            "ambiguous_function_names",
            DEFAULT_AMBIGUOUS_FUNCTION_NAMES,
        )
        min_generic_lines = _threshold(self.config, "min_generic_function_lines", 30)
        if name in ambiguous_names and function_lines >= min_generic_lines:
            self.issues.append(
                Issue(
                    "ambiguous_function_names",
                    f"ambiguous_function_name:{relative}:{qualified_name}",
                    relative,
                    {
                        "function": qualified_name,
                        "line": node.lineno,
                        "name": node.name,
                        "lines": function_lines,
                        "limit": min_generic_lines,
                    },
                )
            )

    def _check_function_name_length(
        self,
        node: ast.FunctionDef | ast.AsyncFunctionDef,
        qualified_name: str,
        relative: str,
    ) -> None:
        tokens = _split_name_tokens(node.name)
        threshold_name = "max_test_function_name_tokens" if _is_test_function(relative, node.name) else "max_function_name_tokens"
        default_limit = 12 if threshold_name == "max_test_function_name_tokens" else 6
        max_tokens = _threshold(self.config, threshold_name, default_limit)
        if len(tokens) > max_tokens:
            self.issues.append(
                Issue(
                    "function_name_shape",
                    f"function_name_shape:{relative}:{qualified_name}:too_many_tokens",
                    relative,
                    {
                        "function": qualified_name,
                        "line": node.lineno,
                        "name": node.name,
                        "tokens": len(tokens),
                        "limit": max_tokens,
                        "reason": "too_many_tokens",
                    },
                )
            )

    def _check_boolean_function_name(
        self,
        node: ast.FunctionDef | ast.AsyncFunctionDef,
        qualified_name: str,
        relative: str,
    ) -> None:
        if self._has_only_boolean_returns(node):
            if node.name.startswith("__") and node.name.endswith("__"):
                return
            if _is_boolean_method_name(node.name):
                return
            boolean_prefixes = _name_prefixes(self.config, "boolean_prefixes", DEFAULT_BOOLEAN_PREFIXES)
            if not _has_boolean_prefix(node.name, boolean_prefixes):
                self.issues.append(
                    Issue(
                        "boolean_name_mismatch",
                        f"boolean_name_mismatch:{relative}:{qualified_name}:function",
                        relative,
                        {
                            "function": qualified_name,
                            "line": node.lineno,
                            "name": node.name,
                            "reason": "boolean_function_without_predicate_name",
                        },
                    )
                )

    def _check_function_contract(
        self,
        node: ast.FunctionDef | ast.AsyncFunctionDef,
        qualified_name: str,
        function_lines: int,
    ) -> None:
        relative = self.path.relative_to(self.repo_root).as_posix()
        min_docstring_lines = _threshold(self.config, "min_docstring_function_lines", 60)
        is_public_function = not node.name.startswith("_") and not relative.startswith("tests/")
        if (is_public_function or function_lines >= min_docstring_lines) and ast.get_docstring(node) is None:
            self.issues.append(
                Issue(
                    "missing_contract_docstrings",
                    f"missing_contract_docstring:{relative}:{qualified_name}",
                    relative,
                    {
                        "function": qualified_name,
                        "line": node.lineno,
                        "lines": function_lines,
                        "limit": min_docstring_lines,
                        "public": is_public_function,
                    },
                )
            )
        max_parameters = _threshold(self.config, "max_parameters", 8)
        parameter_count = self._parameter_count(node)
        if parameter_count > max_parameters:
            self.issues.append(
                Issue(
                    "too_many_parameters",
                    f"too_many_parameters:{relative}:{qualified_name}",
                    relative,
                    {
                        "function": qualified_name,
                        "line": node.lineno,
                        "parameters": parameter_count,
                        "limit": max_parameters,
                    },
                )
            )

    def _check_function_locals(
        self,
        node: ast.FunctionDef | ast.AsyncFunctionDef,
        qualified_name: str,
        function_lines: int,
    ) -> None:
        """Report local names that obscure type, scope, or mutable state."""
        relative = self.path.relative_to(self.repo_root).as_posix()
        assigned_names: set[str] = {arg.arg for arg in self._iter_args(node)}
        local_name_rules = LocalNameRules(
            ambiguous_names=_name_list(
                self.config,
                "ambiguous_variable_names",
                DEFAULT_AMBIGUOUS_VARIABLE_NAMES,
            ),
            allowed_short_names=_name_list(self.config, "allowed_short_names", DEFAULT_ALLOWED_SHORT_NAMES),
            always_bad_short_names=_name_list(
                self.config,
                "always_bad_short_names",
                DEFAULT_ALWAYS_BAD_SHORT_NAMES,
            ),
            min_vague_scope_lines=_threshold(self.config, "min_ambiguous_variable_scope_lines", 30),
            max_short_scope_lines=_threshold(self.config, "max_single_letter_scope_lines", 15),
        )
        boolean_prefixes = _name_prefixes(self.config, "boolean_prefixes", DEFAULT_BOOLEAN_PREFIXES)

        for local_node in _iter_local_nodes(node):
            self._check_local_node(relative, qualified_name, local_node, assigned_names, boolean_prefixes)

        max_locals = _threshold(self.config, "max_locals", 25)
        if len(assigned_names) > max_locals:
            self.issues.append(
                Issue(
                    "too_many_locals",
                    f"too_many_locals:{relative}:{qualified_name}",
                    relative,
                    {
                        "function": qualified_name,
                        "line": node.lineno,
                        "locals": len(assigned_names),
                        "limit": max_locals,
                    },
                )
            )
        for name in sorted(assigned_names):
            self._check_local_name(
                relative,
                qualified_name,
                node,
                name,
                function_lines,
                local_name_rules,
            )

    def _check_local_node(
        self,
        relative: str,
        qualified_name: str,
        local_node: ast.AST,
        assigned_names: set[str],
        boolean_prefixes: tuple[str, ...],
    ) -> None:
        if isinstance(local_node, ast.Name) and isinstance(local_node.ctx, ast.Store):
            assigned_names.add(local_node.id)
            return
        if isinstance(local_node, ast.Global):
            self._add_global_state_issue(relative, qualified_name, local_node, "global")
            return
        if isinstance(local_node, ast.Nonlocal):
            self._add_global_state_issue(relative, qualified_name, local_node, "nonlocal")
            return
        if isinstance(local_node, ast.Pass):
            self._add_pass_placeholder_issue(relative, qualified_name, local_node, "pass")
            return
        if _is_ellipsis_expr(local_node):
            self._add_pass_placeholder_issue(relative, qualified_name, local_node, "ellipsis")
            return
        if isinstance(local_node, ast.Compare):
            self._check_literal_boolean_compare(relative, qualified_name, local_node)
            return
        if isinstance(local_node, (ast.Assign, ast.AnnAssign, ast.NamedExpr)):
            self._check_assignment_names(relative, qualified_name, local_node, boolean_prefixes)

    def _check_local_name(
        self,
        relative: str,
        qualified_name: str,
        node: ast.FunctionDef | ast.AsyncFunctionDef,
        name: str,
        function_lines: int,
        local_name_rules: LocalNameRules,
    ) -> None:
        if name in BUILTIN_NAMES and not (name.startswith("__") and name.endswith("__")):
            self.issues.append(
                Issue(
                    "builtin_shadowing",
                    f"builtin_shadowing:{relative}:{qualified_name}:{name}",
                    relative,
                    {"function": qualified_name, "line": node.lineno, "name": name},
                )
            )
        if name in local_name_rules.always_bad_short_names or (
            len(name) == 1
            and name not in local_name_rules.allowed_short_names
            and function_lines > local_name_rules.max_short_scope_lines
        ):
            self.issues.append(
                Issue(
                    "single_letter_names",
                    f"single_letter_name:{relative}:{qualified_name}:{name}",
                    relative,
                    {
                        "function": qualified_name,
                        "line": node.lineno,
                        "name": name,
                        "lines": function_lines,
                        "limit": local_name_rules.max_short_scope_lines,
                    },
                )
            )
        if function_lines >= local_name_rules.min_vague_scope_lines and name in local_name_rules.ambiguous_names:
            self.issues.append(
                Issue(
                    "ambiguous_variable_names",
                    f"ambiguous_variable_name:{relative}:{qualified_name}:{name}",
                    relative,
                    {
                        "function": qualified_name,
                        "line": node.lineno,
                        "name": name,
                        "lines": function_lines,
                        "limit": local_name_rules.min_vague_scope_lines,
                    },
                )
            )

    def _check_assignment_names(
        self,
        relative: str,
        qualified_name: str,
        node: ast.Assign | ast.AnnAssign | ast.NamedExpr,
        boolean_prefixes: tuple[str, ...],
    ) -> None:
        if isinstance(node, ast.Assign):
            assigned_names = [name for assignment_target in node.targets for name in _target_names(assignment_target)]
            assigned_value = node.value
        elif isinstance(node, ast.AnnAssign):
            assigned_names = _target_names(node.target)
            assigned_value = node.value
        else:
            assigned_names = _target_names(node.target)
            assigned_value = node.value
        if assigned_value is None:
            return
        collection_kind = _collection_expression_kind(assigned_value)
        for name in assigned_names:
            if _is_bool_expression(assigned_value) and not _has_boolean_prefix(name, boolean_prefixes):
                self.issues.append(
                    Issue(
                        "boolean_name_mismatch",
                        f"boolean_name_mismatch:{relative}:{qualified_name}:{name}:assignment",
                        relative,
                        {
                            "function": qualified_name,
                            "line": getattr(node, "lineno", 1),
                            "name": name,
                            "reason": "boolean_assignment_without_predicate_name",
                        },
                    )
                )
            if collection_kind:
                self._check_collection_name(relative, qualified_name, name, collection_kind, getattr(node, "lineno", 1))

    def _check_collection_name(
        self,
        relative: str,
        qualified_name: str,
        name: str,
        collection_kind: str,
        line: int,
    ) -> None:
        singular_exceptions = _name_list(
            self.config,
            "collection_singular_exceptions",
            DEFAULT_COLLECTION_SINGULAR_EXCEPTIONS,
        )
        dict_markers = _name_prefixes(self.config, "dict_name_markers", DEFAULT_DICT_NAME_MARKERS)
        normalized = name.strip("_")
        if normalized in singular_exceptions:
            return
        if collection_kind == "dict":
            if any(marker in normalized for marker in dict_markers):
                return
        elif normalized.endswith(("s", "_list", "_set", "_ids", "_rows")):
            return
        self.issues.append(
            Issue(
                "collection_name_mismatch",
                f"collection_name_mismatch:{relative}:{qualified_name}:{name}",
                relative,
                {
                    "function": qualified_name,
                    "line": line,
                    "name": name,
                    "collection": collection_kind,
                },
            )
        )

    def _check_literal_boolean_compare(
        self,
        relative: str,
        qualified_name: str,
        node: ast.Compare,
    ) -> None:
        values = [node.left, *node.comparators]
        if not any(_is_boolean_or_none_literal(value) for value in values):
            return
        if not any(isinstance(operator, (ast.Eq, ast.NotEq)) for operator in node.ops):
            return
        self.issues.append(
            Issue(
                "boolean_name_mismatch",
                f"boolean_literal_compare:{relative}:{qualified_name}:{node.lineno}:{node.col_offset}",
                relative,
                {
                    "function": qualified_name,
                    "line": node.lineno,
                    "reason": "literal_boolean_or_none_comparison",
                },
            )
        )

    def _add_global_state_issue(
        self,
        relative: str,
        qualified_name: str,
        node: ast.Global | ast.Nonlocal,
        kind: str,
    ) -> None:
        names = ",".join(node.names)
        self.issues.append(
            Issue(
                "global_state_usage",
                f"global_state_usage:{relative}:{qualified_name}:{kind}:{node.lineno}:{names}",
                relative,
                {"function": qualified_name, "line": node.lineno, "kind": kind, "names": node.names},
            )
        )

    def _add_pass_placeholder_issue(
        self,
        relative: str,
        qualified_name: str,
        node: ast.AST,
        kind: str,
    ) -> None:
        self.issues.append(
            Issue(
                "pass_placeholders",
                f"pass_placeholder:{relative}:{qualified_name}:{kind}:{getattr(node, 'lineno', 1)}",
                relative,
                {"function": qualified_name, "line": getattr(node, "lineno", 1), "kind": kind},
            )
        )

    def _has_only_boolean_returns(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
        """Return true when a function annotation or all returns are boolean."""
        if isinstance(node.returns, ast.Name) and node.returns.id == "bool":
            return True
        returns = [
            child.value
            for child in _iter_without_nested_scopes(node)
            if isinstance(child, ast.Return) and child.value is not None
        ]
        return bool(returns) and all(_is_bool_expression(value) for value in returns)

    def _parameter_count(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> int:
        args = node.args
        return (
            len(args.posonlyargs)
            + len(args.args)
            + len(args.kwonlyargs)
            + (1 if args.vararg else 0)
            + (1 if args.kwarg else 0)
        )

    def _iter_args(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> Iterable[ast.arg]:
        args = node.args
        yield from args.posonlyargs
        yield from args.args
        yield from args.kwonlyargs
        if args.vararg:
            yield args.vararg
        if args.kwarg:
            yield args.kwarg


def _analyze_file(repo_root: Path, path: Path, config: dict[str, Any]) -> list[Issue]:
    relative = path.relative_to(repo_root).as_posix()
    issues: list[Issue] = []
    file_lines = _line_count(path)
    max_file_lines = _threshold(config, "max_file_lines", 500)
    if file_lines > max_file_lines:
        issues.append(
            Issue(
                "long_files",
                f"long_file:{relative}",
                relative,
                {"lines": file_lines, "limit": max_file_lines},
            )
        )
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


def collect_issues(repo_root: Path, config: dict[str, Any]) -> dict[str, list[Issue]]:
    """Collect readability findings grouped by rule category."""
    patterns = _compile_suppression_patterns(config)
    issues_by_category: dict[str, list[Issue]] = {category: [] for category in DEFAULT_ISSUE_CATEGORIES}
    for path in _iter_source_files(repo_root, config):
        for issue in _analyze_file(repo_root, path, config):
            issues_by_category[issue.category].append(issue)
        issues_by_category["inline_suppressions"].extend(_find_inline_suppressions(repo_root, path, patterns))
        if path.suffix == ".py":
            issues_by_category["comment_noise"].extend(_find_comment_noise(repo_root, path, config))
    return {category: sorted(values, key=lambda issue: issue.identifier) for category, values in issues_by_category.items()}


def build_snapshot(repo_root: Path, config: dict[str, Any]) -> dict[str, Any]:
    """Build the deterministic readability snapshot used for gating."""
    issues_by_category = collect_issues(repo_root, config)
    return {
        "version": 1,
        "rules": _rules_snapshot(config),
        "thresholds": config["thresholds"],
        "issues": {
            category: [issue.to_json() for issue in values]
            for category, values in sorted(issues_by_category.items())
        },
        "issue_counts": {
            category: len(values)
            for category, values in sorted(issues_by_category.items())
        },
    }


def _rules_snapshot(config: dict[str, Any]) -> dict[str, Any]:
    return {
        "readability": config.get("readability", {}),
        "thresholds": config.get("thresholds", {}),
    }


def _baseline_snapshot(snapshot: dict[str, Any]) -> dict[str, Any]:
    return {
        "version": snapshot.get("version", 1),
        "rules": snapshot.get("rules", {}),
        "thresholds": snapshot.get("thresholds", {}),
        "issue_counts": snapshot.get("issue_counts", {}),
        "issue_ids": {
            category: sorted(issue["id"] for issue in issues)
            for category, issues in snapshot.get("issues", {}).items()
        },
    }


def _issue_ids(snapshot: dict[str, Any], category: str) -> set[str]:
    issue_ids = snapshot.get("issue_ids", {}).get(category)
    if issue_ids is not None:
        return set(issue_ids)
    return {issue["id"] for issue in snapshot.get("issues", {}).get(category, [])}


def _new_issues(current: dict[str, Any], baseline: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    new_by_category: dict[str, list[dict[str, Any]]] = {}
    for category, current_issues in current.get("issues", {}).items():
        baseline_ids = _issue_ids(baseline, category)
        new_items = [issue for issue in current_issues if issue["id"] not in baseline_ids]
        if new_items:
            new_by_category[category] = new_items
    return new_by_category


def _print_summary(snapshot: dict[str, Any]) -> None:
    print("Readability budget summary:")
    for category, count in sorted(snapshot["issue_counts"].items()):
        print(f"  {category}: {count}")


def _print_new_issues(new_by_category: dict[str, list[dict[str, Any]]]) -> None:
    print("New readability debt found:")
    for category, issues in sorted(new_by_category.items()):
        print(f"  {category}: {len(issues)}")
        for issue in issues[:20]:
            location = f"{issue['path']}:{issue.get('line', 1)}"
            detail = (
                issue.get("function")
                or issue.get("class")
                or issue.get("name")
                or issue.get("reason")
                or issue.get("pattern")
                or issue.get("lines")
                or issue.get("depth")
            )
            print(f"    {location} {detail}")
        if len(issues) > 20:
            print(f"    ... {len(issues) - 20} more")


def parse_args(argv: list[str]) -> argparse.Namespace:
    """Parse CLI flags for checking or refreshing readability baselines."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--config", default=DEFAULT_CONFIG)
    parser.add_argument("--baseline", default=DEFAULT_BASELINE)
    parser.add_argument("--write-baseline", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Run the readability budget check and return a process exit code."""
    args = parse_args(argv or sys.argv[1:])
    repo_root = args.repo_root.resolve()
    config_path = repo_root / args.config
    baseline_path = repo_root / args.baseline
    config = _load_json(config_path)
    snapshot = build_snapshot(repo_root, config)
    _print_summary(snapshot)
    if args.write_baseline:
        baseline_path.write_text(
            json.dumps(_baseline_snapshot(snapshot), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        print(f"Wrote baseline: {baseline_path.relative_to(repo_root)}")
        return 0
    if not baseline_path.exists():
        print(f"Baseline is missing: {baseline_path.relative_to(repo_root)}", file=sys.stderr)
        return 2
    baseline = _load_json(baseline_path)
    if baseline.get("rules", {"thresholds": baseline.get("thresholds")}) != snapshot.get("rules"):
        print("Readability rules changed; regenerate the baseline intentionally.", file=sys.stderr)
        return 2
    new_by_category = _new_issues(snapshot, baseline)
    if new_by_category:
        _print_new_issues(new_by_category)
        return 1
    print("No new readability debt relative to baseline.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
