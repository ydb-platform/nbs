Checks are launched without labels only for [organization members](https://github.com/orgs/ydb-platform/people). Later we plan to limit this only to NBS teams.

If PR is opened not by a team member they will receive a message that the team member needs to label their PR with an `ok-to-test` label. Beware of RCE.

There is also a list of labels that slightly alters how and which tests are run:

1. `large-tests` to launch large tests in PR. By default, we launch small and medium.
2. `blockstore`, `filestore`, `disk_manager`, `tasks`, `storage` to launch test ONLY for specified projects. You can specify more than one label.
3. `sleep` to add 7200s (2 hours) sleep to your run, if you want to debug it.
4. `asan`, `tsan`, `msan`, `ubsan` to add address sanitizer, thread sanitizer, memory sanitizer or undefined behaviour sanitizer builds on top of the regular build.
5. `recheck` trigger checks without commit. Removed automatically after launch.
6. `allow-downgrade` allows to downgrade VM preset dynamically in case of problems with resources
7. `disable_truncate`, by default, `.err`/`.log`/`.out` files are truncated to 1GiB, this disables that for this PR
8. `rebase`, tries to rebase current branch to fresh main

Also, you can launch [ya make](https://github.com/ydb-platform/nbs/actions/workflows/on_demand_build_and_test.yaml) builds on your branch with any timeout you want (but please do not do more than 12 hours, VMs are expensive). You can find the internal IP of the VM in the header of the job in the workflow (e.g. 10.24.0.58). To log into it you must be member of NBS team and use SSH key that you use to commit to Github.

You can use command `ssh -J github@185.82.69.80:2222 github@10.24.0.58` or add following to `.ssh/config`:

```
Host 10.24.*.*
    UserKnownHostsFile /dev/null
    ProxyJump github@185.82.69.80:2222
    User github
```

All build and test workflows provide some level of debug info available on our s3 website.

The example URL for the top-level directory is like this: https://github-actions-s3.storage.eu-north2.nebius.cloud/ydb-platform/nbs/PR-check/25435415943/1/nebius-x86-64/index.html

* `ydb-platform` - name of the organization
* `nbs` - name of the repo
* `PR-check` - ID of the workflow
* `25435415943` - ID of the workflow run (you can look it up in the `Actions` tab in your PR)
* `1` - number of runs, if you restart the workflow this number will increase.
* `nebius-x86-64` - nebius is the prefix and x86-64 is an architecture, there also can be one of the suffixes: `-debug`, `-asan`,`-tsan` etc, if you choose to build with debugging symbols or sanitizers.

For new runs, we generate index.html files only for the top-level directories, not for directories lower. And every night we regenerate index files for the whole s3 bucket.

On the top level, you can expect a directory structure like this:

* `build_logs/` - well, ya make build logs
* `logs/` - log of the tests, short version
* `test_logs/` - logs of ya test
* `test_reports/` - junit report, debug data so you can more or less debug what happened during the preparation of the summary report.
* `summary/` - ya-test.html with results of the test run
* `test_data/` - all test data except binaries, VM images, and everything like that, that take a lot of space on S3. Stored only for a week, while other directories are stored for a month.

Files in `test_data` are synced only for failed tests and the list of folders to sync is determined by [script fail_checker.py](https://github.com/ydb-platform/nbs/blob/main/.github/scripts/tests/fail_checker.py)

Availability of other logs in the summary report is dependent on what ya make decide to add in junit report.

