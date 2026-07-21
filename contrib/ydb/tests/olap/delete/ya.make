PY3TEST()
    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/harness_dep.inc)

    FORK_SUBTESTS()

    TEST_SRCS(
        base.py
        test_delete_by_explicit_row_id.py
        test_delete_all_after_inserts.py
    )

    REQUIREMENTS(cpu:2)
    IF (SANITIZER_TYPE)
        SIZE(LARGE)
        INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
    ELSE()
        SIZE(MEDIUM)
    ENDIF()

    PEERDIR(
        contrib/ydb/tests/library
        contrib/ydb/tests/library/test_meta
        contrib/ydb/tests/olap/common
    )

    DEPENDS(
        )

END()
