UNITTEST_FOR(cloud/filestore/libs/storage/tablet)

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/small.inc)

SRCS(
    tablet_ut_counters.cpp
)

PEERDIR(
    cloud/filestore/libs/storage/testlib
)

YQL_LAST_ABI_VERSION()

END()
