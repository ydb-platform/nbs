PY3TEST()

ENV(YDB_DRIVER_BINARY="contrib/ydb/apps/ydbd/ydbd")

PEERDIR(
    contrib/ydb/public/api/protos
    contrib/ydb/public/sdk/python
    contrib/ydb/public/api/grpc
    contrib/ydb/tests/library
    contrib/ydb/tests/oss/ydb_sdk_import
)

DEPENDS(
    contrib/ydb/apps/ydbd
)

TEST_SRCS(
    test_update_script_tables.py
)

FORK_TEST_FILES()
FORK_SUBTESTS()
SPLIT_FACTOR(10)

IF (SANITIZER_TYPE == "thread")
    TIMEOUT(2400)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

FORK_SUBTESTS()

END()
