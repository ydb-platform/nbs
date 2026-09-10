UNITTEST_FOR(cloud/filestore/libs/storage/service)

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/medium.inc)

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SPLIT_FACTOR(14)
ENDIF()

SRCS(
    ../service_ut_helpers.cpp
    ../service_ut_sharding.cpp
)

PEERDIR(
    cloud/filestore/libs/storage/tablet/events
    cloud/filestore/libs/storage/testlib
    cloud/filestore/private/api/protos

    contrib/ydb/core/testlib
)

YQL_LAST_ABI_VERSION()

END()
