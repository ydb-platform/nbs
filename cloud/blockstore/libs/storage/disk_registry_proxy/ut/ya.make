UNITTEST_FOR(cloud/blockstore/libs/storage/disk_registry_proxy)

SRCS(
    disk_registry_proxy_ut.cpp
)

PEERDIR(
    cloud/blockstore/libs/storage/testlib
)


   YQL_LAST_ABI_VERSION()


REQUIREMENTS(ram:11)

END()
