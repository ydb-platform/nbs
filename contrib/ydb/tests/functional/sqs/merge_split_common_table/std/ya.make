PY3TEST()
ENV(YDB_DRIVER_BINARY="contrib/ydb/apps/ydbd/ydbd")
ENV(SQS_CLIENT_BINARY="contrib/ydb/core/ymq/client/bin/sqs")

TAG(ya:manual)

TEST_SRCS(
    test.py
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
    contrib/ydb/tests/functional/sqs/merge_split_common_table
    contrib/python/xmltodict
    contrib/python/boto3
    contrib/python/botocore
)

END()
