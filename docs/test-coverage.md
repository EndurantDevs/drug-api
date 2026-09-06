# Test Coverage Policy

CI preserves Python line and branch coverage ratios with exact integer
cross-products. Uncovered counts remain diagnostics, not absolute caps:
equally or better covered growth does not require unrelated debt paydown or
extra headroom. Source scope, baseline files, measurement settings, test
selection, and coverage exclusions remain protected.

Existing coverage debt can be reduced in scheduled, focused test work rather
than charged to every product change. The transition keeps all committed
coverage counts unchanged; later failures cannot be fixed by lowering them.

Executable added or modified product lines require 85% coverage. The coverage
scope remains `main.py`, `api/`, `db/`, `process/`, and `service/`.
Tests, scripts, and generated or support data outside that scope are exempt;
pure deletions add no diff denominator. Missing changed product files, malformed
reports, and positive summary counts without corresponding raw line records
fail closed.

## Exact-base evidence

The committed `test-coverage-baseline.json` stores policy and the transition
snapshot. Successful main validation in the private CI control repository
publishes `drug-api-coverage-baseline-<source-sha>` for 90 days, with measured
metrics, files, exact source SHA, and a generated companion `test-coverage.md`.
The public source workflow remains portable import checks only.

Private PR and main checks retrieve successful main evidence for the exact
target base. They verify its source SHA, policy, report identity, metrics, and
source files. Candidate report provenance still binds the exact head, base,
Coverage.py version, report path, and SHA-256 content. Missing, expired, or
mismatched required artifacts fail closed. Only exact base commits predating
`machine_artifact_required` use their committed baseline for bootstrap.

The existing test workload and requirements are unchanged. Historical baseline
policy records Coverage.py 7.15.2 and pytest 9.0.3; this transition preserves
those settings and the separately pinned current requirements. Candidate report
provenance verifies the actual Coverage.py runtime version. GitHub CI remains
the required full-validation gate.

## Committed transition snapshot

This generated table mirrors committed bootstrap metrics, not a fresh main
measurement. The source-bound machine artifact is authoritative for current
metrics; its companion table is generated from that artifact's JSON.

<!-- coverage-baseline:start -->
| Metric | Covered / total | Coverage |
| --- | ---: | ---: |
| Branches | 171 / 634 | 26.97% |
| Lines | 1,511 / 2,815 | 53.68% |

<!-- coverage-baseline:end -->

## Development feedback

Use focused checks for changed behavior. The lightweight policy self-test and
documentation consistency check need no services:

```bash
python scripts/coverage_ratchet.py --self-test
python scripts/coverage_reports.py --check
```

`scripts/coverage_forecast.py forecast --help` describes forecasting existing
verified reports. Private CI supplies the optional `--reference-baseline` and
`--baseline-output` paths when the candidate enables the machine protocol.
Only a successful forecast emits the measured baseline JSON and matching
documentation beside it. Diagnostics retain uncovered branch arcs and report
ratio deltas plus executable changed-line coverage. Forecasting neither reruns
the test workload nor updates tracked baselines.
