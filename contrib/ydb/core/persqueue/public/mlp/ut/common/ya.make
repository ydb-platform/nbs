LIBRARY()

SRCS(
    common.cpp
)

PEERDIR(
    contrib/ydb/core/persqueue/public/mlp
    contrib/ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils
    contrib/ydb/public/sdk/cpp/src/client/topic/ut/ut_utils
    contrib/ydb/public/sdk/cpp/src/client/query
    library/cpp/testing/unittest
)

YQL_LAST_ABI_VERSION()

END()
