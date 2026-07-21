TEST_SRCS(
    contrib/ydb/tests/functional/sqs/messaging/test_generic_messaging.py
    contrib/ydb/tests/functional/sqs/messaging/test_fifo_messaging.py
    contrib/ydb/tests/functional/sqs/messaging/test_polling.py
)

DATA(
    arcadia/contrib/ydb/tests/functional/sqs/messaging
)

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
    REQUIREMENTS(ram:32 cpu:1)
ELSE()
    SIZE(MEDIUM)
    REQUIREMENTS(cpu:2)
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
