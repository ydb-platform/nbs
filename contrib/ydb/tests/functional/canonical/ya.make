PY3TEST()

ENV(YDB_TOKEN="root@builtin")
ENV(YDB_ADDITIONAL_LOG_CONFIGS="GRPC_SERVER:DEBUG,TICKET_PARSER:WARN,KQP_COMPILE_ACTOR:DEBUG")
TEST_SRCS(
    test_sql.py
)
IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/ydbd_dep.inc)
DEPENDS(
)


DATA (
    arcadia/contrib/ydb/tests/functional/canonical/canondata
    arcadia/contrib/ydb/tests/functional/canonical/sql
)


PEERDIR(
    contrib/ydb/tests/library
    contrib/ydb/tests/oss/canonical
    contrib/ydb/tests/oss/ydb_sdk_import
)

FORK_SUBTESTS()
FORK_TEST_FILES()

END()
