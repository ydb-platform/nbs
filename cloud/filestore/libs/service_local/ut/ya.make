UNITTEST_FOR(cloud/filestore/libs/service_local)

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/small.inc)

PEERDIR(
    cloud/storage/core/libs/aio
)

SRCS(
    config_ut.cpp
    index_ut.cpp
    service_ut.cpp
)

IF (OS_LINUX)
    SRCS(
        lowlevel_ut.cpp
    )
ENDIF()

END()
