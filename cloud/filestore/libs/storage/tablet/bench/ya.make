Y_BENCHMARK()

NO_SANITIZE()

SRCS(
    tablet_bench.cpp
)

PEERDIR(
    cloud/filestore/libs/storage/testlib
)

PEERDIR(
    cloud/filestore/libs/storage/tablet
)

YQL_LAST_ABI_VERSION()

END()
