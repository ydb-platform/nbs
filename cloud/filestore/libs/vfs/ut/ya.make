UNITTEST_FOR(cloud/filestore/libs/vfs)

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/small.inc)

SRCS(
    fsync_queue_ut.cpp
)

PEERDIR(
    cloud/filestore/libs/diagnostics
)

END()
