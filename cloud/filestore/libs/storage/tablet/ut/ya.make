UNITTEST_FOR(cloud/filestore/libs/storage/tablet)

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/medium.inc)
INCLUDE(${ARCADIA_ROOT}/cloud/filestore/libs/storage/tablet/ut/srcs.inc)

SRCS(
    tablet_ut_external_payload.cpp
    tablet_ut_filesystem_events.cpp
)

END()
