TEST_SRCS(
    contrib/ydb/tests/functional/sqs/common/test_account_actions.py
    contrib/ydb/tests/functional/sqs/common/test_acl.py
    contrib/ydb/tests/functional/sqs/common/test_counters.py
    contrib/ydb/tests/functional/sqs/common/test_multiplexing_tables_format.py
    contrib/ydb/tests/functional/sqs/common/test_ping.py
    contrib/ydb/tests/functional/sqs/common/test_queue_attributes_validation.py
    contrib/ydb/tests/functional/sqs/common/test_queues_managing.py
    contrib/ydb/tests/functional/sqs/common/test_format_without_version.py
    contrib/ydb/tests/functional/sqs/common/test_throttling.py
    contrib/ydb/tests/functional/sqs/common/test_queue_counters.py
    contrib/ydb/tests/functional/sqs/common/test_queue_tags.py
)

DATA(
    arcadia/contrib/ydb/tests/functional/sqs/common
)

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
    REQUIREMENTS(ram:32 cpu:4)
ELSE()
    SIZE(MEDIUM)
    REQUIREMENTS(cpu:2)
ENDIF()

DEPENDS(
)

PEERDIR(
    contrib/ydb/tests/library
    contrib/ydb/tests/library/sqs
    contrib/ydb/tests/oss/ydb_sdk_import
    contrib/python/xmltodict
    contrib/python/boto3
    contrib/python/botocore
)

FORK_SUBTESTS()
SPLIT_FACTOR(60)
