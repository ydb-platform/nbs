UNITTEST_FOR(cloud/filestore/libs/storage/tablet)

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/medium.inc)
INCLUDE(${ARCADIA_ROOT}/cloud/filestore/libs/storage/tablet/ut/srcs.inc)

ENV(NFS_FORCE_RDMA=1)

END()
