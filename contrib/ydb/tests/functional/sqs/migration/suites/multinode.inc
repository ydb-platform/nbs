TEST_SRCS(
    contrib/ydb/tests/functional/sqs/multinode/test_multinode_cluster.py
    contrib/ydb/tests/functional/sqs/multinode/test_recompiles_requests.py
)

DATA(
    arcadia/contrib/ydb/tests/functional/sqs/multinode
)

REQUIREMENTS(cpu:4)

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
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
SPLIT_FACTOR(60)
