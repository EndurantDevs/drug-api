import json
import sys
import textwrap
from importlib import util
from pathlib import Path

SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "readability_budget.py"
SPEC = util.spec_from_file_location("readability_budget", SCRIPT_PATH)
readability_budget = util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = readability_budget
SPEC.loader.exec_module(readability_budget)

NOQA_FIXTURE = "# no" + "qa: E123"
COMMENT_NOISE_FIXTURE = "# return" + " result"


def _write_config(repo_root: Path) -> None:
    config_dict = {
        "source_roots": ["pkg"],
        "include_suffixes": [".py"],
        "exclude_globs": [],
        "thresholds": {
            "max_file_lines": 8,
            "max_function_lines": 4,
            "max_nesting_depth": 1,
            "max_function_name_tokens": 6,
            "max_class_name_tokens": 6,
            "min_generic_function_lines": 4,
            "min_ambiguous_variable_scope_lines": 4,
            "max_single_letter_scope_lines": 2,
            "min_docstring_function_lines": 4,
            "max_parameters": 3,
            "max_locals": 3,
        },
        "readability": {
            "ambiguous_function_names": ["process_data"],
            "ambiguous_class_names": ["Manager"],
            "ambiguous_variable_names": ["data", "row", "result"],
            "allowed_short_names": ["_", "i"],
            "always_bad_short_names": ["l", "O"],
            "boolean_prefixes": ["is_", "has_", "should_"],
            "dict_name_markers": ["_by_", "_map"],
            "collection_singular_exceptions": ["data"],
            "comment_noise_patterns": ["^return\\b"],
        },
        "inline_suppression_patterns": [
            {"name": "python_noqa", "pattern": "#\\s*noqa\\b"},
        ],
    }
    (repo_root / "readability-budget.json").write_text(json.dumps(config_dict), encoding="utf-8")


def _assert_issue_count(snapshot: dict, category: str, expected_count: int) -> None:
    assert snapshot["issue_counts"][category] == expected_count


def test_readability_budget_allows_existing_debt(tmp_path):
    repo_root = tmp_path
    package = repo_root / "pkg"
    package.mkdir()
    (package / "module.py").write_text(
        textwrap.dedent(
            """
            def existing():
                return 1  {noqa_fixture}
            """
        ).format(noqa_fixture=NOQA_FIXTURE),
        encoding="utf-8",
    )
    _write_config(repo_root)

    expected_exit_code = 0
    assert readability_budget.main(["--repo-root", str(repo_root), "--write-baseline"]) == expected_exit_code
    assert readability_budget.main(["--repo-root", str(repo_root)]) == expected_exit_code


def test_readability_budget_rejects_new_inline_suppression(tmp_path):
    repo_root = tmp_path
    package = repo_root / "pkg"
    package.mkdir()
    module = package / "module.py"
    module.write_text("def clean():\n    return 1\n", encoding="utf-8")
    _write_config(repo_root)
    expected_baseline_exit_code = 0
    assert readability_budget.main(["--repo-root", str(repo_root), "--write-baseline"]) == expected_baseline_exit_code

    module.write_text(
        textwrap.dedent(
            """
            def clean():
                return 1

            def new_debt():
                return 2  {noqa_fixture}
            """
        ).format(noqa_fixture=NOQA_FIXTURE),
        encoding="utf-8",
    )

    expected_failure_exit_code = 1
    assert readability_budget.main(["--repo-root", str(repo_root)]) == expected_failure_exit_code


def test_readability_budget_reports_long_functions(tmp_path):
    repo_root = tmp_path
    package = repo_root / "pkg"
    package.mkdir()
    (package / "module.py").write_text(
        textwrap.dedent(
            """
            def too_long():
                first = 1
                second = 2
                third = 3
                fourth = 4
                return first + second + third + fourth
            """
        ),
        encoding="utf-8",
    )
    _write_config(repo_root)

    snapshot = readability_budget.build_snapshot(
        repo_root,
        json.loads((repo_root / "readability-budget.json").read_text(encoding="utf-8")),
    )

    _assert_issue_count(snapshot, "long_functions", 1)
    assert snapshot["issues"]["long_functions"][0]["function"] == "too_long"


def test_descriptive_test_names_are_allowed(tmp_path):
    repo_root = tmp_path
    test_package = repo_root / "tests"
    test_package.mkdir()
    (test_package / "test_module.py").write_text(
        textwrap.dedent(
            """
            def test_product_lookup_returns_empty_payload_for_unknown_rxnorm_id():
                assert True
            """
        ),
        encoding="utf-8",
    )
    config_dict = {
        "source_roots": ["tests"],
        "include_suffixes": [".py"],
        "exclude_globs": [],
        "thresholds": {
            "max_file_lines": 20,
            "max_function_lines": 20,
            "max_nesting_depth": 2,
            "max_function_name_tokens": 6,
        },
        "inline_suppression_patterns": [],
    }

    snapshot = readability_budget.build_snapshot(repo_root, config_dict)

    _assert_issue_count(snapshot, "function_name_shape", 0)


def test_readability_budget_ignores_numeric_comparisons(tmp_path):
    repo_root = tmp_path
    package = repo_root / "pkg"
    package.mkdir()
    (package / "module.py").write_text(
        textwrap.dedent(
            """
            def is_clean_exit_code_check():
                exit_code = 0
                count = 1
                return exit_code == 0 and count == 1
            """
        ),
        encoding="utf-8",
    )
    _write_config(repo_root)

    snapshot = readability_budget.build_snapshot(
        repo_root,
        json.loads((repo_root / "readability-budget.json").read_text(encoding="utf-8")),
    )

    _assert_issue_count(snapshot, "boolean_name_mismatch", 0)


def test_readability_budget_ignores_fallback_assignments(tmp_path):
    repo_root = tmp_path
    package = repo_root / "pkg"
    package.mkdir()
    (package / "module.py").write_text(
        textwrap.dedent(
            """
            def display_label(primary, fallback):
                label = primary or fallback or ""
                return label
            """
        ),
        encoding="utf-8",
    )
    _write_config(repo_root)

    snapshot = readability_budget.build_snapshot(
        repo_root,
        json.loads((repo_root / "readability-budget.json").read_text(encoding="utf-8")),
    )

    _assert_issue_count(snapshot, "boolean_name_mismatch", 0)


def test_readability_budget_ignores_isoformat_assignments(tmp_path):
    repo_root = tmp_path
    package = repo_root / "pkg"
    package.mkdir()
    (package / "module.py").write_text(
        textwrap.dedent(
            """
            import datetime

            def timestamp_text():
                started_at = datetime.datetime.utcnow().isoformat()
                return started_at
            """
        ),
        encoding="utf-8",
    )
    _write_config(repo_root)

    snapshot = readability_budget.build_snapshot(
        repo_root,
        json.loads((repo_root / "readability-budget.json").read_text(encoding="utf-8")),
    )

    _assert_issue_count(snapshot, "boolean_name_mismatch", 0)


def test_readability_budget_does_not_parse_non_python_files(tmp_path):
    repo_root = tmp_path
    package = repo_root / "pkg"
    package.mkdir()
    (package / "route.rs").write_text(
        "fn main() {\n    println!(\"not python\");\n}\n",
        encoding="utf-8",
    )
    config_dict = {
        "source_roots": ["pkg"],
        "include_suffixes": [".py", ".rs"],
        "exclude_globs": [],
        "thresholds": {
            "max_file_lines": 8,
            "max_function_lines": 4,
            "max_nesting_depth": 1,
        },
        "inline_suppression_patterns": [],
    }
    (repo_root / "readability-budget.json").write_text(json.dumps(config_dict), encoding="utf-8")

    snapshot = readability_budget.build_snapshot(repo_root, config_dict)

    _assert_issue_count(snapshot, "syntax_errors", 0)


def test_readability_budget_reports_naming_and_contract_debt(tmp_path):
    repo_root = tmp_path
    package = repo_root / "pkg"
    package.mkdir()
    (package / "module.py").write_text(
        textwrap.dedent(
            """
            class Manager:
                pass

            def process_data(a, b, c, d):
                {comment_noise_fixture}
                data = [1, 2, 3]
                row = {{"a": 1}}
                result = a == b
                l = 1
                extra = 2
                another = 3
                return result
            """
        ).format(comment_noise_fixture=COMMENT_NOISE_FIXTURE),
        encoding="utf-8",
    )
    _write_config(repo_root)

    snapshot = readability_budget.build_snapshot(
        repo_root,
        json.loads((repo_root / "readability-budget.json").read_text(encoding="utf-8")),
    )

    _assert_issue_count(snapshot, "ambiguous_function_names", 1)
    _assert_issue_count(snapshot, "ambiguous_variable_names", 3)
    _assert_issue_count(snapshot, "boolean_name_mismatch", 1)
    _assert_issue_count(snapshot, "class_name_shape", 1)
    _assert_issue_count(snapshot, "comment_noise", 1)
    _assert_issue_count(snapshot, "missing_contract_docstrings", 1)
    _assert_issue_count(snapshot, "single_letter_names", 5)
    _assert_issue_count(snapshot, "too_many_locals", 1)
    _assert_issue_count(snapshot, "too_many_parameters", 1)


def test_readability_budget_reports_collection_and_global_state_debt(tmp_path):
    repo_root = tmp_path
    package = repo_root / "pkg"
    package.mkdir()
    (package / "module.py").write_text(
        textwrap.dedent(
            """
            def build_lookup():
                global CACHE
                names = {"a": 1}
                thing = []
                ...
            """
        ),
        encoding="utf-8",
    )
    _write_config(repo_root)

    snapshot = readability_budget.build_snapshot(
        repo_root,
        json.loads((repo_root / "readability-budget.json").read_text(encoding="utf-8")),
    )

    _assert_issue_count(snapshot, "collection_name_mismatch", 2)
    _assert_issue_count(snapshot, "global_state_usage", 1)
    _assert_issue_count(snapshot, "pass_placeholders", 1)
