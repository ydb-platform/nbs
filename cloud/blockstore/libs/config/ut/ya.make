# BlockStore configuration and overlay unit tests.
# The test target also exercises integration with the dynamic control board.

UNITTEST_FOR(cloud/blockstore/libs/config)

SRCS(
    blockstore_config_ut.cpp
)

PEERDIR(
    contrib/ydb/core/control
)

END()
