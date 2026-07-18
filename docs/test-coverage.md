# Test Coverage Ratchet

CI records coverage as exact covered and total counts. A change fails when its
coverage ratio falls or when its uncovered-code count rises. This lets coverage
improve while preventing the current debt from growing.

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
Run `python scripts/coverage_ratchet.py --self-test` to exercise the gate.
