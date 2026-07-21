TEST_SRCS(
    contrib/ydb/tests/functional/sqs/large/test_leader_start_inflight.py
)

DATA(
    arcadia/contrib/ydb/tests/functional/sqs/large
)

REQUIREMENTS(cpu:4)
IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
    REQUIREMENTS(ram:32)
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
