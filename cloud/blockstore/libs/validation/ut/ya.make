UNITTEST_FOR(cloud/blockstore/libs/validation)

INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/tests/recipes/small.inc)

SRCS(
    validation_client_ut.cpp
    validation_service_ut.cpp
)

END()
