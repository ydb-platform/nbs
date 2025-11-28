PY3TEST()
ENV(YDB_DRIVER_BINARY="contrib/ydb/apps/ydbd/ydbd")
ENV(SQS_CLIENT_BINARY="contrib/ydb/core/ymq/client/bin/sqs")

TAG(ya:manual)

TEST_SRCS(
    test_multinode_cluster.py
    test_recompiles_requests.py
)

IF (SANITIZER_TYPE == "thread")
    TIMEOUT(2400)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

REQUIREMENTS(
    cpu:4
    ram:32
)

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

# SQS tests are not CPU or disk intensive,
# but they use sleeping for some events,
# so it would be secure to increase split factor.
# This increasing of split factor reduces test time
# to 15-20 seconds.
SPLIT_FACTOR(60)

END()
