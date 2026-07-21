PY3TEST()

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/tools/fq_runner/ydb_runner_with_datastreams.inc)
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/fq/streaming_common/vm_metadata_emulator/recipe/recipe.inc)
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/fq/streaming_common/iam_grpc_emulator/recipe/recipe.inc)

TEST_SRCS(
    test_iam.py
    test_scalar_topic_write.py
    test_streaming.py
    test_watermarks.py
)

IF (OS_LINUX)
    TEST_SRCS(
        test_udfs.py
    )
ENDIF()

PY_SRCS(
    conftest.py
)

REQUIREMENTS(cpu:4)
IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
    REQUIREMENTS(ram:20)
ELSE()
    SIZE(MEDIUM)
    FORK_SUBTESTS()
ENDIF()

PEERDIR(
    contrib/ydb/tests/library
    contrib/ydb/tests/library/test_meta
    contrib/ydb/public/sdk/python
    contrib/ydb/public/sdk/python/enable_v3_new_behavior
    library/recipes/common
    contrib/ydb/tests/olap/common
    contrib/ydb/tests/tools/datastreams_helpers
    contrib/ydb/tests/fq/streaming_common
)

DEPENDS(
    contrib/ydb/apps/ydb
    contrib/ydb/tests/tools/pq_read
    contrib/ydb/library/yql/udfs/common/python/python3_small
)

END()
