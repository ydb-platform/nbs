PY3TEST()
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/harness_dep.inc)
ENV(YDB_CLI_BINARY="contrib/ydb/apps/ydb/ydb")

FORK_TEST_FILES()
FORK_TESTS()
FORK_SUBTESTS()
SPLIT_FACTOR(30)

TEST_SRCS(
    test_example.py
    test_followers.py
    test_compatibility.py
    test_system_views.py
    test_data_type.py
    test_ctas.py
    test_batch_operations.py
    test_node_broker_delta_protocol.py
    test_system_tablet_backup.py
    test_user_management.py
    test_default_columns.py
    test_infer_pdisk_expected_slot_count.py
    test_show_create_table.py
    test_snapshot_isolation.py
)

SIZE(LARGE)
REQUIREMENTS(cpu:4)
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)

DEPENDS(
    contrib/ydb/tests/library/compatibility/binaries
    contrib/ydb/apps/ydb
)

PEERDIR(
    contrib/ydb/tests/library
    contrib/ydb/tests/datashard/lib
    contrib/ydb/tests/library/compatibility
)

END()
