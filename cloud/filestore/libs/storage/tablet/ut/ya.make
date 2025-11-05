UNITTEST_FOR(cloud/filestore/libs/storage/tablet)

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/medium.inc)

SRCS(
    helpers_ut.cpp
    rebase_logic_ut.cpp
    subsessions_ut.cpp
    tablet_database_ut.cpp
    tablet_ut.cpp
    tablet_ut_cache.cpp
    tablet_ut_channels.cpp
    tablet_ut_checkpoints.cpp
    tablet_ut_data.cpp
    tablet_ut_data_allocate.cpp
    tablet_ut_data_truncate.cpp
    tablet_ut_data_zerorange.cpp
    tablet_ut_handles.cpp
    tablet_ut_monitoring.cpp
    tablet_ut_nodes.cpp
    tablet_ut_nodes_filteralivenodes.cpp
    tablet_ut_sessions.cpp
    tablet_ut_subsessions.cpp
    tablet_ut_throttling.cpp
    tablet_ut_state_cache.cpp
)

PEERDIR(
    cloud/filestore/libs/storage/testlib
    contrib/ydb/core/testlib
)

YQL_LAST_ABI_VERSION()

END()
