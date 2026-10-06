UNITTEST_FOR(cloud/filestore/libs/storage/disk_registry_proxy/impl)

IF (SANITIZER_TYPE)
    INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/medium.inc)
ELSE()
    INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/small.inc)
ENDIF()

SRCS(
    disk_registry_proxy_ut.cpp
)

PEERDIR(
    cloud/filestore/libs/storage/testlib

    contrib/ydb/core/tablet_flat
    contrib/ydb/core/testlib
)

YQL_LAST_ABI_VERSION()

END()
