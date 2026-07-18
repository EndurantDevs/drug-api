# Readability Budget

This repo uses `scripts/readability_budget.py` to keep readability debt visible
and prevent new debt from entering unnoticed.

## Rules

- Do not add inline suppression comments such as `# noqa`, `# type: ignore`,
  `# pylint: disable`, or Rust `#[allow(...)]`.
- Use names that tell the human what role a value plays. Avoid generic
  function names, vague long-scope locals, one-letter names outside tiny scopes,
  and names that shadow Python builtins.
- Do not distinguish one-item and multi-item functions only with a trailing
  plural; make them differ by at least one full semantic token. Prefer keeping
  the singular entity name for one item and adding cardinality or a collection
  verb for many items, such as `refresh_node_health` and
  `refresh_node_health_batch` rather than `refresh_nodes_health`. When a stable
  multi-item API must remain, qualify the private helper by role or action, such
  as `_format_credential_row` or `_publish_single_ndc_table`.
- Externally defined protocol methods keep their required spellings. The checker
  uses narrow built-in exemptions for known families such as SQLAlchemy-style
  `scalar()` and `scalars()` methods.
- Boolean names should read as predicates: `is_*`, `has_*`, `should_*`,
  `can_*`, `needs_*`, `supports_*`, or equivalent.
- Collection names should reveal their shape: plural names for lists/sets and
  `_by_*`, `_map`, `_lookup`, `_index`, or similar names for dictionaries.
- Long or public functions should have a contract docstring, not comments that
  merely repeat the next line.
- Fix warnings with clearer code, narrower types, smaller functions, or better
  tests.
- Exclude generated, cache, build, local data, or runtime artifact paths only in
  `readability-budget.json`.
- Keep importers decomposed by source discovery, download, parse, stage, publish,
  and materialize phases.

## Thresholds

- Source files over 500 lines are reported.
- Python functions over 60 lines are reported.
- Python nesting deeper than 4 control-flow levels is reported.
- Inline suppressions are reported and blocked when new.
- Naming and decomposition debt is reported for generic function/class names,
  vague local variable names in long scopes, boolean-name mismatches, builtin
  shadowing, confusable singular/plural function names, collection-name
  mismatches, one-letter names, too many parameters,
  too many locals, global/nonlocal state, missing contract docstrings, placeholder
  bodies, and noisy comments.

Existing debt IDs are stored in `readability-baseline.json`. The CI check fails
only when new debt appears relative to that baseline. When debt is removed or the
rules intentionally change, regenerate the baseline in the same change:

```bash
python scripts/readability_budget.py --write-baseline
```

Normal local check:

```bash
python scripts/readability_budget.py
```
