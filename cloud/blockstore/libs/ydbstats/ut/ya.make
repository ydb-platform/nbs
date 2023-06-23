UNITTEST_FOR(cloud/blockstore/libs/ydbstats)

SRCS(
    ydbstats_ut.cpp
    ydbwriters_ut.cpp
)

PEERDIR(
    ydb/core/testlib/default
)

END()
