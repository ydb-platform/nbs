UNITTEST_FOR(cloud/filestore/libs/vfs)

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/medium.inc)

SRCS(
    fsync_queue_ut_stress.cpp
)

PEERDIR(
    cloud/filestore/libs/diagnostics
)

END()
