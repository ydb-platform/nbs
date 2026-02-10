UNITTEST_FOR(cloud/blockstore/libs/local_nvme)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    generic_grpc_device_provider_ut.cpp
    test_grpc_device_provider.cpp
)

IF (OS_LINUX)
    SRCS(
        service_linux_ut.cpp
    )
ENDIF(OS_LINUX)

PEERDIR(
    cloud/blockstore/tools/testing/infra-device-provider/protos
    cloud/storage/core/libs/grpc
    library/cpp/testing/unittest
)

SET_APPEND(RECIPE_ARGS --devices cloud/blockstore/libs/local_nvme/ut_infra/devices.yaml)
INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/tests/recipes/infra-device-provider/recipe.inc)

END()
