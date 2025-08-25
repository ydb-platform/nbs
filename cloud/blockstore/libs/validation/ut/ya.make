UNITTEST_FOR(cloud/blockstore/libs/validation)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    data_integrity_client_ut.cpp
    validation_client_ut.cpp
    validation_service_ut.cpp
)

END()
