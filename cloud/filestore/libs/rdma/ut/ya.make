UNITTEST_FOR(cloud/filestore/libs/rdma)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    endpoint_ut.cpp
    protocol_ut.cpp
)

PEERDIR(
    cloud/filestore/libs/rdma_test
    cloud/filestore/private/api/protos

    cloud/storage/core/libs/common
    cloud/storage/core/libs/diagnostics
    cloud/storage/core/libs/rdma/iface

    library/cpp/threading/future
)

END()
