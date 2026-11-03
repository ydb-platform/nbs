LIBRARY()

SRCS(
    dynconfig.cpp
)

PEERDIR(
    cloud/blockstore/libs/config
    cloud/blockstore/libs/configs_manager

    contrib/ydb/core/testlib/actors

    library/cpp/testing/unittest
)

END()
