# Test Coverage Ratchet

CI records coverage as exact covered and total counts. A change fails when its
coverage ratio falls or when its uncovered-code count rises. This lets coverage
improve while preventing the current debt from growing.

Production-source changes also pay down existing debt until the configured
target is reached. For every metric in the affected language report, CI requires
the smaller of:

- 1% of the base branch's uncovered units, rounded up; or
- 10% of the PR's changed in-scope source lines, rounded up, with a minimum of one.

Lines and statements target 95%. Branches, functions, and compiler regions
target 90%. Test-only, documentation, and tooling PRs keep the exact
no-regression check without an unrelated paydown requirement. The policy and
targets are versioned in `test-coverage-baseline.json`; CI rejects attempts to
weaken them.

## Current baseline

Measured with Coverage.py 7.15.2 across `main.py`, `api/`, `db/`,
`process/`, and `service/`:

| Metric | Covered / total | Coverage |
| --- | ---: | ---: |
| Lines | 1,511 / 2,815 | 53.68% |
| Branches | 171 / 634 | 26.97% |

## Local check

```bash
python -m coverage run --branch --source=. -m pytest -q
python -m coverage json -o test-coverage-python.json
python scripts/coverage_ratchet.py
```

The versioned source of truth is `test-coverage-baseline.json`. Do not lower it
manually: CI compares changes with the baseline on the pull request base commit.
After an in-scope source change, regenerate the report and run
`python scripts/coverage_ratchet.py --report python --write-baseline` so the
required improvement becomes the next PR's floor. The pinned Ubuntu CI
measurement is canonical; if local counts differ, use the CI counts rather than
committing platform-specific totals.
Run `python scripts/coverage_ratchet.py --self-test` to exercise the gate.
