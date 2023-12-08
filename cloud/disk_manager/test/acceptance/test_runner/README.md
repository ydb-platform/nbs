# Acceptance test runner

## Configuration

- `verify-test` can be built from sources [verify_test_sources](https://a.yandex-team.ru/arc/trunk/arcadia/cloud/blockstore/tools/testing/verify-test).
- `acceptance-test` can be built from sources [acceptance_test_sources](https://a.yandex-team.ru/arc/trunk/arcadia/cloud/disk_manager/test/acceptance).

1. Install `ycp` and build `acceptance-test` (see [setup_instruction](https://a.yandex-team.ru/arc/trunk/arcadia/cloud/disk_manager/test/acceptance)).
2. Build `verify-test`:
    ```(bash)
    $ ya make ../../../../blockstore/tools/testing/verify-test/
    ```
3. Build `test-runner`:
    ```(bash)
    $ ya make
    ```

## "acceptance" test type

### Description
1. Creates instance.
2. Copies `verify-test` to created instance (via `sftp`).
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
$ ./disk-manager-ci-acceptance-test-suite --cluster hw-nbs-stable-lab --acceptance-test <path_to_arcadia>/cloud/disk_manager/test/acceptance/acceptance-test --instance-cores 16 --instance-ram 16 --verbose acceptance --test-suite default --verify-test <path_to_arcadia>/cloud/blockstore/tools/testing/verify-test/verify-test
```

### Leaked resources
There is a possibility when the test runner can leave leaked resources inside the cloud. So if it is happened:
1. Check and remove all leaks of `acceptance-test` with suffix `acceptance` (see [leakes_instruction](https://a.yandex-team.ru/arc/trunk/arcadia/cloud/disk_manager/test/acceptance)).
2. Look to the `disk-artifact` file and delete all disks from there (if there is no any `disk-artifact` file, try to manually find and delete disks, created by `acceptance-test` with the instruction from the first step).
3. If you add `--conserve-snapshots` flag, than look to the `snapshot-artifact` file and delete all snapshots from there.
4. Find instance inside the cluster with the name `acceptance-test-acceptance-<test-suite>-<timestamp>` and delete it manually.
5. Find disk inside the cluster with the name `acceptance-test-acceptance-<test-suite>-<timestamp>` and delete it manually.

## "eternal" test type

### Description
1. Creates instance.
2. Tries to find disk with name regex `acceptance-test-eternal-[0-9]+(b|kib|mib|gib|tib)-[0-9]+(b|kib|mib|gib|tib)-[0-9]+` (first number is the test disk size, second - blocksize).
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
$ ./disk-manager-ci-acceptance-test-suite --cluster hw-nbs-stable-lab --acceptance-test <path_to_arcadia>/cloud/disk_manager/test/acceptance/acceptance-test --instance-cores 16 --instance-ram 16 --verbose eternal --disk-size 1024 --cmp-util <path_to_arcadia>/cloud/disk_manager/test/acceptance/cmp/acceptance-cmp
```

### Leaked resources
There is a possibility when the test runner can leave leaked resources inside the cloud. So if it is happened:
1. Check and remove all leaks of `acceptance-test` with suffix `eternal` (see [leakes_instruction](https://a.yandex-team.ru/arc/trunk/arcadia/cloud/disk_manager/test/acceptance)).
2. Look to the `disk-artifact` file and delete all disks from there (if there is no any `disk-artifact` file, try to manually find and delete disks, created by `acceptance-test` with the instruction from the first step).
3. If you add `--conserve-snapshots` flag, than look to the `snapshot-artifact` file and delete all snapshots from there.
4. Find instance inside the cluster with the name `acceptance-test-eternal-<timestamp>` and delete it manually.
5. Find disk inside the cluster with the name `acceptance-test-eternal-<disk_size>-<disk_blocksize>-<timestamp>` and delete it manually.

## "sync" test type

### Description
1. Creates instance.
2. Tries to find disk with name regex `acceptance-test-sync-disk-[0-9]+(b|kib|mib|gib|tib)-[0-9]+(b|kib|mib|gib|tib)-[0-9]+` (first number is the test disk size, second - blocksize).
3. If disk wasn't found on previous step, then creates it.
4. Attaches disk to instance.
5. Creates 3 files in disk, fills them with random data from /dev/random
6. Remembers files checksums
7. To be continued...

**NOTE:** Source disk is not deleted within all the test. It must be deleted manually!

### Example
```(bash)
$ ./yc-disk-manager-ci-acceptance-test-suite --cluster hw-nbs-stable-lab --acceptance-test <path_to_arcadia>/cloud/disk_manager/test/acceptance/acceptance-test --instance-cores 16 --instance-ram 16 --verbose sync --disk-size 1024
```

### Leaked resources
There is a possibility when the test runner can leave leaked resources inside the cloud. So if it is happened:
1. Check and remove all leaks of `acceptance-test` with suffix `sync` (see [leakes_instruction](https://a.yandex-team.ru/arc/trunk/arcadia/cloud/disk_manager/test/acceptance)).
2. Look to the `disk-artifact` file and delete all disks from there (if there is no any `disk-artifact` file, try to manually find and delete disks, created by `acceptance-test` with the instruction from the first step).
3. If you add `--conserve-snapshots` flag, than look to the `snapshot-artifact` file and delete all snapshots from there.
4. Find instance inside the cluster with the name `acceptance-test-sync-<timestamp>` and delete it manually.
5. Find disk inside the cluster with the name `acceptance-test-sync-<disk_size>-<disk_blocksize>-<timestamp>` and delete it manually.
