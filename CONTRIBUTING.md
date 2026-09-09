# Contributing

Work from the repo root in an activated virtualenv. Keep changes focused.

## Contributor License Agreement

External pull requests must pass CLA Assistant before they can be merged. By
submitting a contribution, you confirm that you have read and can accept the
[EndurantDevs Individual Contributor License Agreement](CLA.md).

CLA Assistant uses the canonical public CLA Gist:
https://gist.github.com/dnikolayev/ed619a73b0095cbb30de041cd6ca4421

If your employer, client, university, or another organization may own rights in
your contribution, make sure you are authorized to contribute before opening a
pull request.

## Branches

Create feature and fix branches from `dev` and open normal pull requests into
`dev`. Run focused local checks for the changed behavior; GitHub CI is the
full-validation gate. After merge, maintainers verify the development deployment
and its behavior before considering the change accepted.

Once `dev` is stable, maintainers prepare a release to `main` only when a human
requests it. Promote reviewed content through a release pull request using
**Rebase and merge**. Passing CI or merging into `dev` does not itself request a
production release. The public default branch remains `main`.

For a security update proposed against `main`, maintainers first port the change
to `dev` and complete the normal validation and release flow. Preserve the
original pull request until the reviewed replacement is linked.

Use `type/short-slug` names: `feature/<slug>`, `fix/<slug>`,
`docs/<slug>`, `test/<slug>`, or `chore/<slug>`.

## Commit Messages

Use the style in [docs/commit-messages.md](docs/commit-messages.md). Run
`python3 scripts/check_commit_messages.py --last 1` before pushing
hand-written commits.

## Tests

Run the focused tests for the area you changed before opening a pull request.
For importer or schema changes, include a smoke run or a short note explaining
why one was not run.

Use the [local setup](README.md#local-setup) to install contributor dependencies.
For example, validate synthetic NDC normalization and publication behavior with:

```bash
python -m pytest -q tests/process/test_ndc_rxnorm_mapping.py \
  tests/process/test_import_table_switching.py
```

These tests do not require PostgreSQL, Redis or source downloads. API contract
checks are available with `python -m pytest -q tests/test_openapi_spec.py`.
GitHub CI is the full-validation gate; no local aggregate gate is required.
