UNITTEST_FOR(contrib/ydb/core/fq/libs/control_plane_proxy)

SIZE(MEDIUM)

PEERDIR(
    library/cpp/testing/unittest
    contrib/ydb/core/base
    contrib/ydb/core/fq/libs/actors/logging
    contrib/ydb/core/fq/libs/control_plane_storage
    contrib/ydb/core/fq/libs/test_connection
    contrib/ydb/core/fq/libs/quota_manager/ut_helpers
    contrib/ydb/core/fq/libs/rate_limiter/control_plane_service
    contrib/ydb/core/testlib/default
    contrib/ydb/library/folder_service
    contrib/ydb/library/folder_service/mock
)

YQL_LAST_ABI_VERSION()

SRCS(
    control_plane_proxy_ut.cpp
)

END()
