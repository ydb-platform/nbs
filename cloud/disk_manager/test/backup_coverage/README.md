# Differential Go statement coverage for NBS-7923

This tool accounts for the union of executable statements added/changed by:
- #7175: 872fb1aea9a3badbcd879363658e64e7bf836056 -> 11bf043f6b6055c4ded599421075afb4af4f4b3b
- #7222: 8ce7b078a944dbe1e2b3eb55f73a7d7b47dbdfd9 -> f8fe79c76cae19902d1c092edc636fd92d44c5ee

It is pinned to reviewed semantic mappings at HEAD
8fb8ddf4aea80eb85fdbf5993d8c02f19cbb56d0. Later revisions with changed product bytes need a mapping review; test-only
commits can reuse a mapping after exact product-file byte comparison.
Product files must match the mapped Git revision; test-only edits are allowed.

From the repository root, collect unit/component profiles with the normal ya
test recipes, debug, -j64, --go-coverage and --coverage-prefix-filter=cloud/disk_manager.
Use the narrow Backup test filters in the acceptance manifest. Instrumentation
covers the tested Go package, not all imported packages: backup, dataplane,
services/images, services/snapshots, snapshot/storage, its chunks/schema
subpackages and resources each need their own profile. Never include E2E-only
coverage or sum package percentages.

Archive each go.coverage.tar before another run replaces test-results. Record
its SHA-256, execution UUID, successful test result, product source revision,
test source hashes, toolchain and recipe. The tool checks syntax and computes
coverage; provenance and successful execution are separate acceptance evidence.

```sh
python3 cloud/disk_manager/test/backup_coverage/selftest.py --go /absolute/path/to/go
python3 cloud/disk_manager/test/backup_coverage/differential_coverage.py \
  --repo /absolute/path/to/checkout --go /absolute/path/to/go \
  --work /absolute/path/to/report --profiles /absolute/path/to/unit/go.coverage.tar
```

Pass all applicable archives after --profiles. Exit 0 requires >=90%, no unknown
historical mappings and exact block accounting. A missing profile is uncovered,
never excluded. With no profiles, the gate fails. No build/test is launched by
the report command; Go's AST parser and cover instrumenter run on saved sources.

scope.json records statements introduced by each PR. report.json contains
numerator, denominator, percentage, all mixed blocks (old vs new counts), raw
profile hashes, exclusions, reviewed rename/move/modified mappings, retired
statements, unknowns and uncovered positions. Statement positions are checked
against Go cover NumStmt, including closures in package initializers.
A mixed counter contributes only its new statements, and its hit applies to
those statements according to Go's statement coverage semantics.

Token matching is restricted to the same function. Explicit mappings are
checked against historical/current revision, position, kind and token hash.
Deletion of the old scheduling stub and first-PR statements replaced by the
second PR is pinned in reviewed_retired_statements.json; arbitrary retirements
fail closed. Replaced statements are counted once. Existing unchanged code,
tests, mocks, generated Go/proto and declarative build configuration are excluded.

At the tested revision, 548 / 602 = 91.0299003322259% using 20 successful
unit/component archives. The 54 uncovered statements remain in the denominator,
including app/admin wiring and uncommon storage errors. This percentage does not
prove the external fault matrix; those scenarios have independent evidence.

To collect the full narrow Backup suite from scratch:

```sh
./ya make cloud/disk_manager/internal/pkg/dataplane/tests cloud/disk_manager/internal/pkg/dataplane/backup/tests cloud/disk_manager/internal/pkg/dataplane/tasks_tests cloud/disk_manager/internal/pkg/services/snapshots/tests cloud/disk_manager/internal/pkg/services/snapshots/tasks_tests cloud/disk_manager/internal/pkg/services/images/tests cloud/disk_manager/internal/pkg/services/images/tasks_tests cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/tests cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/chunks/tests cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/schema/tests cloud/disk_manager/internal/pkg/resources/tests --build=debug -j64 -tt --go-coverage --coverage-prefix-filter=cloud/disk_manager '--test-filter=*Backup*' '--test-filter=*ClearCompletedBackupChunks*'
```
