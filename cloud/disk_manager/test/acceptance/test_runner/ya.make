PY3_PROGRAM(disk-manager-ci-acceptance-test-suite)

PY_SRCS(
    __main__.py
    acceptance_test_runner.py
    base_acceptance_test_runner.py
    cleanup.py
    eternal_acceptance_test_runner.py
    main.py
    sync_acceptance_test_runner.py
)

PEERDIR(
    cloud/disk_manager/test/acceptance/test_runner/lib
    ydb/tests/library
)

END()

RECURSE(
    lib
)

RECURSE_FOR_TESTS(tests)
