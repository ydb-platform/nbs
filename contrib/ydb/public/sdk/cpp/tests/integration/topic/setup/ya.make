LIBRARY()

SRCS(
    fixture.cpp
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/src/client/discovery
    contrib/ydb/public/sdk/cpp/src/client/topic
    contrib/ydb/public/sdk/cpp/tests/integration/topic/utils
    library/cpp/testing/gtest
)

END()
