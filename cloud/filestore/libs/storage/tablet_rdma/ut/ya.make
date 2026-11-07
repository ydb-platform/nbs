UNITTEST_FOR(cloud/filestore/libs/storage/tablet_rdma)

IF (SANITIZER_TYPE)
    INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/medium.inc)
ELSE()
    INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/small.inc)
ENDIF()

SRCS(
    rdma_tablet_forwarder_ut.cpp
)

PEERDIR(
    cloud/filestore/libs/storage/testlib
    contrib/ydb/core/testlib
)

YQL_LAST_ABI_VERSION()

END()
