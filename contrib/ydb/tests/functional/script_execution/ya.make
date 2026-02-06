PY3TEST()

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/ydbd_dep.inc)

PEERDIR(
    contrib/ydb/public/api/protos
    contrib/ydb/public/sdk/python
    contrib/ydb/public/api/grpc
    contrib/ydb/tests/library
    contrib/ydb/tests/oss/ydb_sdk_import
)

DEPENDS(
)

TEST_SRCS(
    test_update_script_tables.py
)

FORK_TEST_FILES()
FORK_SUBTESTS()
SPLIT_FACTOR(10)

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

FORK_SUBTESTS()

END()
