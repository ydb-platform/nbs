# Acceptance test runner

These acceptance tests are intended to perform acceptance testing of Disk Manager 
and NBS as a part of the whole cloud. They are intended to run on a separate machine 
with access to lab/production cluster and with internal tool `ycp` installed.
These tests can't be launched locally.

## Terminology

* `Disk` - either a non-replicated disk with mirroring capabilities or YDB blob storage based disks. Disks can be overlay (based on other disks).
* `Snapshot` - snapshot of the disk state, stored either in S3 or in YDB with an ability to provision disks from snapshots. Snapshots can be incremental (based on other snapshots).
* `Image` - images are the same as snapshots with the following exceptions: 
  * Image can be created from the snapshot, snapshot can not be created from images.
  * Disk can be created from either disk or snapshot or image.
  * Images can be optimized for fast provisioning, this way a pool of overlay YDB blob storage based disks is created. Initial image is still stored in S3 or in YDB.

## Test types
* `acceptance` - checks if disks and images are successfully created from snapshots, disks and images. Also contains a test for disk from url creation. 
* `eternal` - checks if disks, images and snapshots are successfully created from each other, but the initial disk is taken from the previous test. 
* `sync` - creates a filesystem on the disk and synchronizes data before snapshot creation, checks if files created from images are the same.

## `acceptance-test` binary
This binary is used in `acceptance` and `eternal` tests and performs the following 3 test suites per each source disk:
1. `mainTest` 
   * creates a snapshot-1 from the source disk
   * creates a snapshot-2 from the same source disk
   * creates an image-1 from the snapshot-1
   * creates disk-1 from the snapshot-1
   * creates disk-2 from image-1
   * creates image-2 from disk-2
   * creates image-3 from image-2
   * removes images, disks and snapshots (if `output-disk-ids` is present disks are conserved, the same goes for snapshots and `output-snapshot-ids`, those parameters are file paths, those files store "\n" separated disk/snapshot ids respectively).
   * In case of the test utility failure, the following entities would remain and not be cleaned up: `acc-image-{suffix}-{timestamp}`, `acc-snapshot-{suffix}-{timestamp}`, `acc-disk-{suffix}-{timestamp}`
2. `createImageFromURLTest`
   * For this test, `url-for-create-image-from-url-test` parameter is required and must be a valid and accessible from the host running the test S3 image url. Image shall be of the following formats: `QCOW2`, `raw`, `VMDK`.
   * Image is created from the S3 URL.
   * Image is deleted afterward. In case of the test failure image is not being cleaned up. Created image has the following format: `acc-image-{suffix}-{timestamp}`, where suffix is optional.
3. `cancelTest`
   * starts snapshot creation operation from the source disk in the background
   * deletes snapshots
   * checks if the snapshot is actually deleted
   * If the test case is interrupted/fails before the snapshot creation is cancelled, the snapshot `acc-snapshot-{suffix}-{timestamp}` would remain.


## Dependencies

To run tests the entrypoint is `disk-manager-ci-acceptance-test-suite`, which initializes virtual machines and performs most testing, whilst using `acceptance-cmp`, `verify-test`, `acceptance-test` and `ycp` utilities under the hood.
`acceptance-test` also uses `ycp` under the hood.
- `verify-test` can be built from sources with `ya make` [verify_test_sources](https://github.com/ydb-platform/nbs/tree/main/cloud/blockstore/tools/testing/verify-test).
- `acceptance-test` can be built from sources with `ya make` [acceptance_test_sources](https://github.com/ydb-platform/nbs/tree/main/cloud/disk_manager/test/acceptance).
- `ycp` (`ycp` is an internal tool, so this tests can't be run in OSS environment)
- `disk-manager-ci-acceptance-test-suite` is the entrypoint. `acceptance-cmp`, `disk-manager-ci-acceptance-test-suite` are built via `ya make` automatically while building the `acceptance-test` target.

## `acceptance` test type

### Description
1. Creates instance.
2. Copies `verify-test` to the created instance (via `sftp`).
3. For every test_case from chosen test_suite:
    1. Creates disk with specified test_case parameters.
    2. Attaches disk to instance.
    3. Fills disk with verification data (remotely via `verify-test`) and checks it.
    4. Detaches disk from instance.
    5. Performs acceptance test to disk (locally via `acceptance-test`) which generates `disk-artifacts` file.
    6. For every artifact disk from `disk-artifact`:
        1. Attaches artifact disk to instance.
        2. Performs verification read to artifact disk (remotely via `verify-test`).
        3. Detaches artifact disks.
    7. Deletes all artifact disks.
    8. Detaches disk from instance.
    9. Deletes disk.
4. Deletes instance.

### Example
```(bash)
$ ./disk-manager-ci-acceptance-test-suite --cluster <cluster> --acceptance-test <repo_root>/cloud/disk_manager/test/acceptance/acceptance-test --instance-cores 16 --instance-ram 16 --verbose acceptance --test-suite default --verify-test <repo_root>/cloud/blockstore/tools/testing/verify-test/verify-test
```

## `eternal` test type

### Description
1. Creates instance.
2. Tries to find disk with name regex `acc-eternal-[0-9]+(b|kib|mib|gib|tib)-[0-9]+(b|kib|mib|gib|tib)-[0-9]+` (first number is the test disk size, second - blocksize).
3. If disk wasn't found on previous step, then creates it.
4. Attaches disk to instance.
5. Fills disk with random data (remotely via `fio`).
6. Performs acceptance test to disk (locally via `acceptance-test`) which generates `disk-artifacts` file.
7. For every artifact disk from `disk-artifact`:
    1. Attaches artifact disk to instance.
    2. Performs verification check with source disk (remotely via `cmp`).
    3. Detaches artifact disks.
8. Deletes all artifact disks.
9. Detaches disk from instance.
10. Deletes instance.

**NOTE:** Source disk is not deleted within all the test. It must be deleted manually!

### Example
```(bash)
$ ./disk-manager-ci-acceptance-test-suite --cluster <cluster> --acceptance-test <repo_root>/cloud/disk_manager/test/acceptance/acceptance-test --instance-cores 16 --instance-ram 16 --verbose eternal --disk-size 1024 --cmp-util <repo_root>/cloud/disk_manager/test/acceptance/cmp/acceptance-cmp
```

## `sync` test type

### Description
1. Creates instance.
2. Tries to find disk with name regex `acc-sync-disk-[0-9]+(b|kib|mib|gib|tib)-[0-9]+(b|kib|mib|gib|tib)-[0-9]+` (first number is the test disk size, second - blocksize).
3. If disk wasn't found on previous step, then creates it.
4. Attaches disk to instance.
5. Creates ext4 filesystem on the disk and mounts the disk to the `/tmp` subdirectory.
6. Creates 3 files on the disk, fills them with random data from `/dev/random`.
7. Saves those file's checksums into a local variable.
8. Creates a snapshot with name `sync-acc-snapshot-<timestamp>` from the disk with files.
9. Creates a disk `acc-sync-<size>-<block-size>-<timestamp>-from-snapshot` from the snapshot
10. Attaches the disk and compares files checksums

**NOTE:** Source disk is not deleted after the test is finished. It must be deleted manually!

### Example
```(bash)
$ ./disk-manager-ci-acceptance-test-suite --cluster <cluster> --acceptance-test <repo_root>/cloud/disk_manager/test/acceptance/acceptance-test --instance-cores 16 --instance-ram 16 --verbose sync --disk-size 1024
```

### Leaked resources and cleanup process:
Here's the list of the resources and regular expressions matching those resources:
* For `acceptance` tests: 
  * For instances: `^acc-acceptance-(small|medium|big|enormous)-[0-9]+$`
  * For disks:
    - `^acc-acceptance-(small|medium|big|enormous)-[0-9]+$`
    - `^acc-disk-acceptance-[0-9]+$` (These disks are created by previous tests versions)
    - `^acc-disk-acceptance-[0-9]+(tib|gib|mib|kib|b)-[0-9]+(tib|gib|mib|kib|b)-[0-9]+$`
  * For images:
    - `^acc-image-acceptance-[0-9]+$` (These images are created by previous tests versions)
    - `^acc-image-acceptance-[0-9]+(tib|gib|mib|kib|b)-[0-9]+(tib|gib|mib|kib|b)-[0-9]+$`
  * For snapshots:
    - `^acc-snapshot-acceptance-[0-9]+$` (These snapshots are created by previous tests versions)
    - `^acc-snapshot-acceptance-[0-9]+(tib|gib|mib|kib|b)-[0-9]+(tib|gib|mib|kib|b)-[0-9]+$`
* For `eternal` tests
  * For instances: `^acc-eternal-[0-9]+$`
  * For disks:
    * `^acc-eternal-[0-9]+(b|kib|mib|gib|tib)-[0-9]+(b|kib|mib|gib|tib)-[0-9]+$` (This disk is a base disk)
    * `^acc-disk-eternal-[0-9]+$` (These disks are created by previous tests versions)
    * `^acc-disk-eternal-[0-9]+(tib|gib|mib|kib|b)-[0-9]+(tib|gib|mib|kib|b)-[0-9]+$`
  * For images:
    * `^acc-image-eternal-[0-9]+$` (These images are created by previous tests versions)
    * `^acc-image-eternal-[0-9]+(tib|gib|mib|kib|b)-[0-9]+(tib|gib|mib|kib|b)-[0-9]+$`
  * For snapshots:
    * `^acc-snapshot-eternal-[0-9]+$` (These snapshots are created by previous tests versions)
    * `^acc-snapshot-eternal-[0-9]+(tib|gib|mib|kib|b)-[0-9]+(tib|gib|mib|kib|b)-[0-9]+$`
* For sync tests:
  * For instances: `^acc-sync-[0-9]+$`
  * For disks:
    * `^acc-sync-[0-9]+(b|kib|mib|gib|tib)-[0-9]+(b|kib|mib|gib|tib)-[0-9]+` (This disk is a base disk)
    * `^acc-sync-[0-9]+(b|kib|mib|gib|tib)-[0-9]+(b|kib|mib|gib|tib)-[0-9]+-from-snapshot$`
  * For snapshots: `^sync-acc-snapshot-.*$`
Note that for the `sync` and `eternal` tests, at least on "base" disk should be kept present, because sync and eternal tests check incremental snapshots.
