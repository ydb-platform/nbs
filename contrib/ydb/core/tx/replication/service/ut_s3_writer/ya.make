UNITTEST_FOR(contrib/ydb/core/tx/replication/service)

FORK_SUBTESTS()

SIZE(MEDIUM)
IF (SANITIZER_TYPE)
    REQUIREMENTS(cpu:2)
ENDIF()

PEERDIR(
    contrib/ydb/core/wrappers/ut_helpers
    contrib/ydb/core/tx/replication/ut_helpers
    contrib/ydb/core/tx/replication/ydb_proxy
    contrib/ydb/core/util
    contrib/ydb/library/aws_init
    library/cpp/string_utils/base64
    library/cpp/testing/unittest
)

SRCS(
    s3_writer_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
