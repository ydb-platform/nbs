PY3TEST()

TEST_SRCS(
    test_common.py
    test_yandex_cloud_mode.py
    test_yandex_cloud_queue_counters.py
)

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/ydbd_dep.inc)

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    TAG(ya:fat)
    REQUIREMENTS(ram:32 cpu:2)
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
SPLIT_FACTOR(40)

END()
