# Readability Budget

This repo uses `scripts/readability_budget.py` to keep readability debt visible
and prevent new debt from entering unnoticed.

## Rules

- Do not add inline suppression comments such as `# noqa`, `# type: ignore`, or
  `# pylint: disable`.
- Fix warnings with clearer code, narrower types, smaller functions, or better
  tests.
- Exclude generated, cache, or runtime artifact paths only in
  `readability-budget.json`.
- Keep route handlers and importer shutdown/publish paths decomposed into named
  helpers that explain each step.

## Thresholds

- Files over 500 lines are reported.
- Functions over 60 lines are reported.
- Nesting deeper than 4 control-flow levels is reported.
- Inline suppressions are reported and blocked when new.

Existing debt is stored in `readability-baseline.json`. The CI check fails only
when new debt appears relative to that baseline. When debt is removed, regenerate
the baseline in the same change:

```bash
python scripts/readability_budget.py --write-baseline
```

Normal local check:

```bash
python scripts/readability_budget.py
```
