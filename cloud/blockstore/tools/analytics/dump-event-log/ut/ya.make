UNITTEST_FOR(cloud/blockstore/tools/analytics/dump-event-log)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    dataset_output.cpp
    dataset_output_ut.cpp
)

PEERDIR(
    cloud/blockstore/libs/diagnostics/events
    cloud/blockstore/libs/service
)

END()
