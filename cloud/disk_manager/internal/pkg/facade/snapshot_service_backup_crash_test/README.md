# Restart a data-plane worker after the backup store accepts a chunk

This isolated recipe starts the normal service processes and local storage emulators. The backup object store is behind a loopback fault proxy. No cloud buckets or shared hosts are used.

1. Start Nemesis in `--controlled-nemesis` mode. Random restarts are disabled. Each service process has a generation-counter file initially set to zero.
2. Create a disk with known data. Configure the proxy to forward a chunk PUT but hold the successful response.
3. Create the snapshot through the API. While the single data-plane worker is running, wait for both an accepted PUT and `held_after_success > 0`. The latter is an active held response, not a historical success counter.
4. Atomically increment that worker's trigger counter. Nemesis cancels the process, waits for `cmd.Wait`, records termination, and starts its replacement. The supervisor and control plane remain running.
5. Require the replacement PID, disappearance of the old PID, and restart acknowledgement. The interval from the first PUT to confirmed restart is limited to 20 seconds; the object-store request timeout is 30 seconds. The fault must still be active and the final chunk map absent.
6. Reset the fault, wait for normal backup recovery, and verify metadata, chunk map, checksums, and every byte of disk data.

An ambiguous attempt fails; it is not retried until an unrelated restart happens to pass. Faults have a TTL and cleanup reset.

```bash
./ya make -ttt -j8 \
  cloud/disk_manager/internal/pkg/facade/snapshot_service_backup_crash_test

./ya make -tt -j8 \
  cloud/tasks/test/nemesis/tests \
  cloud/disk_manager/test/mocks/s3_fault_proxy/tests
```

Without `--restart-trigger-file`, Nemesis retains its existing random mode. The new flag affects only the test helper, not the task runtime.
