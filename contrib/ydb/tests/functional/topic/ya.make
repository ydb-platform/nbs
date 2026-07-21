PY3TEST()

FORK_SUBTESTS()
FORK_TEST_FILES()
SPLIT_FACTOR(100)
SIZE(MEDIUM)

ENV(YDB_USE_IN_MEMORY_PDISKS=true)
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/harness_dep.inc)

TEST_SRCS(
    conftest.py
    helpers.py
    test_topic_audit.py
)

DATA(
    arcadia/contrib/ydb/tests/functional/topic/canondata
)

PEERDIR(
    contrib/python/protobuf
    contrib/ydb/core/persqueue/public/cloud_events/proto
    contrib/ydb/tests/library
    contrib/ydb/tests/library/fixtures
    contrib/ydb/tests/oss/canonical
)

END()
