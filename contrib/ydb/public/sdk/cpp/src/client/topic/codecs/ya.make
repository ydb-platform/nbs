LIBRARY()

SRCS(
    GLOBAL codecs.cpp
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/src/library/kafka
    contrib/ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic
)

END()
