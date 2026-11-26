UNITTEST_FOR(cloud/blockstore/libs/storage/partition)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/medium.inc)

SRCS(
    part_cleanup_logic_ut.cpp
    part_compaction_logic_ut.cpp
    part_database_ut.cpp
    part_readblobinfo_logic_ut.cpp
    part_state_ut.cpp
    part_ut.cpp
    part_ut_compaction.cpp
    part_ut_compaction_policy.cpp
    part_ut_fresh_blocks.cpp
    part_ut_rebuild_metadata.cpp
    part_ut_trim_fresh_log.cpp
)

PEERDIR(
    cloud/blockstore/libs/storage/testlib
)

YQL_LAST_ABI_VERSION()

END()
