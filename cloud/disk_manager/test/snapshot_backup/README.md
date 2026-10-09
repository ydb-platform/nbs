# Automated snapshot backup verification

This directory contains a credential-free test suite and an opt-in continuous tester for a dedicated, disposable VM. It does not enable backups, deploy services, or create cloud infrastructure during a build.

## Test layers

| Layer | Execution environment | Success criterion |
| --- | --- | --- |
| Python unit tests | Pull request, without cloud credentials | Backup parsing, encryption, decompression, checksums, deadlines, journaling, and safety checks behave as expected. |
| Go component tests | Isolated test recipe | Backup tasks handle object-store failures, deletion races, and replay of persisted state. |
| Go API integration | Local service processes and storage emulators | The API creates a snapshot, the scheduler creates its backup, and the backup remains readable after primary resources are deleted. |
| Fault and restart tests | Isolated recipe only | Transient object-store errors and controlled process restarts do not corrupt the backup. Primary operations continue during backup-store faults. |
| Continuous E2E | Dedicated non-production VM | Synthetic disk data is backed up and restored from the backup bucket onto another disposable disk, with matching whole-disk SHA-256. |

The continuous tester checks four successive disk states: fully populated data, modified and zeroed ranges, an unchanged snapshot, and an all-zero disk. It verifies each backup before creating the next snapshot. This is not a backfill test for an existing snapshot chain.

Every cycle also performs these checks, with separate per-case metrics:

- `after_delete`: after confirmed deletion of all four owned source snapshots, restore the changed snapshot again and compare the complete destination disk with its saved digest.
- `retained`: restore the previous successful cycle's changed snapshot after creating and deleting the new snapshots. Its identity and independent digest survive tester restarts. The first cycle uses its own full snapshot as the initial baseline; it does not claim cross-cycle coverage yet.
- `missing_key` and `wrong_key`, when `require_encryption` is enabled: after a successful full restore, read the same backup with no KEK and with a deliberately different KEK. Only the specific missing-key or AES-GCM key-rejection result counts as success. Network failures, denied bucket access, missing maps and general corruption do not. The supplied secrets and remote objects are never modified.

The retained reference advances only after all checks pass. A disappeared, previously verified map fails immediately; it is not treated as a newly pending backup. A changed bucket, prefix, endpoint, source disk or size blocks execution until the saved reference is reconciled. This rolling reference checks persistence across cycles, not long-term archival retention or key rotation.

An independent Python reader restores the data; the test does not call a server-side restore API. The reader receives only backup-bucket access, checks snapshot identity and disk size, validates the complete chunk map, verifies CRC32 and authenticated decryption, and compares the restored whole-disk SHA-256 with an independently generated reference.

## Run code-change checks

From the repository root:

```bash
python3 -m venv /tmp/snapshot-backup-unit-venv
/tmp/snapshot-backup-unit-venv/bin/pip install -r cloud/disk_manager/test/snapshot_backup/requirements.txt
/tmp/snapshot-backup-unit-venv/bin/python -B -m unittest discover -s cloud/disk_manager/test/snapshot_backup/tests -v

./ya make -tt -j8 \
  cloud/tasks/test/nemesis/tests \
  cloud/disk_manager/test/mocks/s3_fault_proxy/tests \
  cloud/disk_manager/internal/pkg/dataplane/tasks_tests \
  cloud/disk_manager/internal/pkg/services/snapshots/tasks_tests

./ya make -ttt -j8 \
  cloud/disk_manager/internal/pkg/facade/snapshot_service_backup_test \
  cloud/disk_manager/internal/pkg/facade/snapshot_service_backup_encryption_test \
  cloud/disk_manager/internal/pkg/facade/snapshot_service_backup_nemesis_test \
  cloud/disk_manager/internal/pkg/facade/snapshot_service_backup_s3_fault_test \
  cloud/disk_manager/internal/pkg/facade/snapshot_service_backup_crash_test
```

The Python pull-request workflow needs no secrets. After a successful check it publishes a source archive named `snapshot-backup-tester-<commit>.tar.gz` with `SHA256SUMS`. This is the tested source revision, not a deployed daemon or VM image.

The five Go integration targets use the existing approval-gated PR matrix. They are LARGE tests and require `-ttt`, not `-tt`. For the initial matrix change, use the existing `disk_manager` and `large-tests` labels and the normal CI approval process. Required-check settings remain a repository-administration task.

### Strict regression targets

These tests are separate from the required passing suite:

```bash
./ya make -tt -j8 cloud/disk_manager/internal/pkg/dataplane/backup_regressions_tests

./ya make -ttt -j8 \
  cloud/disk_manager/internal/pkg/facade/snapshot_service_backup_retry_exhaustion_test \
  --test-filter '*TestSnapshotServiceAutomaticBackupAfterRetryExhaustion*'
```

They require queue progress past missing chunks, complete inherited data before child backup completion, and automatic recovery after terminal retry exhaustion. Failures are release concerns, not accepted behavior. Assertions are not disabled or weakened. See the target READMEs for reproduction and the distinction between observed failures and expected failures.

The random-restart suite and controlled-crash suite serve different purposes. The controlled test requires proof that the object store accepted a chunk, its successful response is still held, and the specific data-plane process handling it exited and was replaced before the request timeout. An ambiguous restart is a failure, not a successful test.

## Continuous E2E prerequisites

The runtime requires a deployment-specific adapter implementing [the provider protocol](PROVIDER.md). No provider CLI, internal endpoint, account, infrastructure provisioning template, or credentials are shipped here. Prepare the adapter and its private runbook outside this public repository.

Use a new VM dedicated to destructive synthetic testing, never a service VM. The initial resource envelope is:

- Linux with systemd, Python 3.11+, curl 8.4+, and `lsblk`; 2 vCPU, 4 GiB RAM, and a 20 GiB boot disk.
- Two new, unmounted 1 GiB data disks, without partitions or user data, exposed as `/dev/disk/by-id/virtio-backup-src` and `/dev/disk/by-id/virtio-backup-dst`.
- An instance label `purpose=snapshot-backup-e2e`, matching instance/disk/scope/zone IDs, and the metadata identity endpoint described in the provider protocol.
- Noninteractive credentials limited to reading this VM and its disks and creating, reading, and deleting synthetic snapshots in a dedicated test scope.
- Separate read-only access to the backup bucket. The tester must not administer identities, encryption keys, shared VMs, or buckets.
- The backup service already configured for that bucket. The tester does not enable it.
- Backup KEK files indexed by key ID. Each key must contain exactly the 32 raw bytes expected by the reader; Base64 text is not decoded automatically. Keep old key IDs during rotation.
- A local metrics collector, expected-tester inventory, and an alert receiver.

Only `testing` and `preprod` environment labels are accepted. The source disk is not encrypted by this test; backup encryption is supported.

### 1. Install a tested revision and the private adapter

Use a successful source artifact for the intended commit. Verify `SHA256SUMS` and preserve the source directory structure under `/opt/snapshot-backup-tester`. Use a main-branch release for a persistent installation; a PR artifact is for a controlled candidate check. Do not run automatic source pulls inside the daemon.

Install through your approved image, package, or configuration-management process. The following are package-install steps on the new disposable VM, not instructions to bypass host access restrictions:

```bash
cd /opt/snapshot-backup-tester
python3 -m venv venv
venv/bin/pip install -r cloud/disk_manager/test/snapshot_backup/requirements.txt
venv/bin/python -B -m unittest discover -s cloud/disk_manager/test/snapshot_backup/tests -v
curl --version
```

Install and independently verify the private adapter. Check its authentication, response normalization, timeouts, mutation semantics, and trace locations. Do not enable the daemon yet.

### 2. Configure identity, access, and secrets

Copy `deploy/config.example.json` to `/etc/snapshot-backup-tester/config.json` and replace every placeholder. Both data disks must match `size_bytes`. The object `prefix` must match the backup service configuration; it is not an arbitrary tester directory.

Set the absolute `provider_command` and `provider_config` paths. The compute and storage profiles are opaque adapter-specific names. Enumerate **every** provider API trace directory in `provider_trace_paths`; the example is not a discovered path for your adapter. The runtime refuses calls when these paths are writable, redirected through symlinks, or cannot be verified. The adapter must not write requests, responses, credentials, or signed URLs elsewhere.

Deliver actual KEKs and credentials through your secret-management mechanism. Never commit them or real deployment configs. Use a private configuration directory and `0600` secret files:

```bash
install -d -m 0700 /var/lib/snapshot-backup-tester
chmod 0700 /etc/snapshot-backup-tester
chmod 0600 /etc/snapshot-backup-tester/config.json /etc/snapshot-backup-tester/provider.json
```

For an explicitly unencrypted smoke test, set `require_encryption: false` and `key_files: {}`. That run does not validate encrypted backups.

At 1 GiB and four snapshots per cycle, 24 cycles can retain roughly 96 GiB of uncompressed backup data plus metadata per VM. The durable attempt budget survives daemon restarts. Establish storage retention before increasing it.

### 3. Preflight and run one cycle inside the sandbox

Provider CLIs can write file traces even with console diagnostics disabled. The systemd sandbox and trace-path checks are mandatory for real calls. Do not run `once` directly from an unrestricted root shell.

```bash
cd /opt/snapshot-backup-tester
install -m 0644 cloud/disk_manager/test/snapshot_backup/deploy/snapshot-backup-tester@.service \
  /etc/systemd/system/snapshot-backup-tester@.service
systemctl daemon-reload
systemctl start snapshot-backup-tester@preflight.service
systemctl show snapshot-backup-tester@preflight.service -p Result -p ExecMainStatus
```

Preflight checks local metadata identity, provider identity, test-purpose label, disk attachments, sizes, partitions, and mountpoints. It does not create a snapshot or prove backup-bucket access.

The next command **overwrites both configured disposable data disks**:

```bash
systemctl start snapshot-backup-tester@once.service
systemctl show snapshot-backup-tester@once.service -p Result -p ExecMainStatus
venv/bin/python -B -m cloud.disk_manager.test.snapshot_backup \
  --config /etc/snapshot-backup-tester/config.json metrics
```

Exit `0` means all four states, post-deletion and retained restores, and any required negative-key checks passed, and owned snapshots were removed. Exit `1` means backup validation failed or its deadline expired. Exit `2` means execution was blocked by configuration, access, API errors, or unresolved resources. A blocked run is not a pass. Inspect the private state journal without publishing sensitive values.

### 4. Enable continuous execution and monitoring

After a successful single cycle:

```bash
install -m 0644 cloud/disk_manager/test/snapshot_backup/deploy/snapshot-backup-tester.service \
  /etc/systemd/system/snapshot-backup-tester.service
systemctl daemon-reload
systemctl enable --now snapshot-backup-tester
systemctl status snapshot-backup-tester --no-pager
curl --fail --silent http://127.0.0.1:9799/metrics
```

The unit allows raw I/O only to the two configured devices. If credentials require a writable renewal cache, permit only a dedicated private path; do not remove the sandbox. Verify a complete cycle under the final unit.

Cycles are sequential, with a 300-second delay after completion and a default 1800-second cycle deadline. Metrics remain available while the child is active or stalled. A child exit without a fresh matching durable result is blocked, never an inherited pass.

Normal cancellation unwinds the active call and terminates its adapter process group. If a cycle is hard-killed, the supervisor exits instead of starting another cycle: the mandatory hardened unit's `KillMode=control-group` clears remaining descendants before service restart. Running the supervisor outside this unit does not provide that guarantee.

Scrape localhost port 9799 every 30 seconds. Adapt `deploy/alerts.example.yaml` to your collector and real receiver. Include overall and per-case status, last success/completion, heartbeat, attempts, failures, latched failure, and remaining budget. Check expected inventory per environment/zone: an aggregate absent-job check cannot detect one missing VM when others remain.

Test alert delivery by stopping **only the tester**, then restart it. Also test a blocked configuration on that tester. An exposed metrics endpoint alone does not prove notification delivery. Service revision inventory is not implemented; do not label a run as validation of a new service version without separate revision evidence.

## Failure handling and cleanup

A failure sets a durable latch; later success does not silently clear it. Known owned snapshot IDs are revalidated before deletion. An uncertain create result blocks new resources until an operator reconciles the journalled name, scope, disk, and ownership label. Never delete the journal to resume: it contains ownership records and the attempt budget.

With the continuous unit stopped, run `snapshot-backup-tester@cleanup.service` for known resources. A missing resource or uncertain deletion needs reconciliation; arbitrary API failures are not treated as successful cleanup. Run `snapshot-backup-tester@once.service` after resolving the cause, then `snapshot-backup-tester@ack-failure.service` only after a pass.

The tester never deletes VMs, data disks, or bucket objects. Backup chunks may be shared, so snapshot-prefix deletion and bucket-wide lifecycle changes are not safe cleanup substitutes.

## Remaining limits

- Server-side restore API, historical-chain backfill, and selective backup are not covered.
- Strict regressions do not fix runtime defects or make a passing main suite sufficient for release.
- The API availability checks do not implement the separate 100-operation/p95 load criterion.
- Provider adapter deployment, credentials, VMs, collectors, and alert receivers require deployment-specific work and a real smoke run.
- Maximum disk size and memory profiling are deferred.

Fault injection, shared-data deletion, and process disruption belong only in the isolated recipe. Continuous real-environment tests operate on their own synthetic resources without disrupting other workloads.
