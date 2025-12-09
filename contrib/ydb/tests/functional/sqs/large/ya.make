PY3TEST()
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/ydbd_dep.inc)
ENV(YDB_USE_IN_MEMORY_PDISKS=true)

TEST_SRCS(
    test_leader_start_inflight.py
)

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
    REQUIREMENTS(ram:32 cpu:4)
ELSE()
    SIZE(MEDIUM)
ENDIF()

DEPENDS(
)

PEERDIR(
    contrib/ydb/tests/library
    contrib/ydb/tests/library/sqs
    contrib/python/xmltodict
    contrib/python/boto3
    contrib/python/botocore
)

FORK_SUBTESTS()


END()
