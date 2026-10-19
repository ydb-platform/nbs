GTEST()

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    ../device_manager_ut.cpp
    ../options_ut.cpp
)

PEERDIR(
    cloud/fastshard/apps/jdserver/lib
    cloud/fastshard/libs/journal/iface
    cloud/fastshard/public/api/protos

    cloud/storage/core/libs/aio
    cloud/storage/core/libs/common
    cloud/storage/core/protos

    contrib/restricted/googletest/googletest
)

END()
