UNITTEST_FOR(cloud/blockstore/libs/storage/partition2)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/medium.inc)

SRCS(
    part2_cleanup_logic_ut.cpp
    part2_compaction_logic_ut.cpp
    part2_database_ut.cpp
    part2_readblobinfo_logic_ut.cpp
    part2_state_ut.cpp
    part2_ut.cpp
)

PEERDIR(
    cloud/blockstore/libs/storage/testlib
)

YQL_LAST_ABI_VERSION()

END()
