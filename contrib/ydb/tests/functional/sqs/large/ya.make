PY3TEST()
ENV(YDB_DRIVER_BINARY="contrib/ydb/apps/ydbd/ydbd")
ENV(SQS_CLIENT_BINARY="contrib/ydb/core/ymq/client/bin/sqs")
ENV(YDB_USE_IN_MEMORY_PDISKS=true)

TAG(ya:manual)

TEST_SRCS(
    test_leader_start_inflight.py
)

IF (SANITIZER_TYPE == "thread")
    TIMEOUT(2400)
    SIZE(LARGE)
    TAG(ya:fat)
    REQUIREMENTS(
        cpu:4
        ram:32
    )
ELSE()
    REQUIREMENTS(
        cpu:4
        ram:32
    )
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

DEPENDS(
    contrib/ydb/apps/ydbd
    contrib/ydb/core/ymq/client/bin
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
