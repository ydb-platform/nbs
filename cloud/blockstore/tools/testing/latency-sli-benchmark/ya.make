PROGRAM(latency-sli-benchmark)

SRCS(main.cpp)

PEERDIR(
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/service
    cloud/contrib/vhost
    cloud/storage/core/libs/vhost-client
)

ADDINCL(cloud/contrib/vhost)

END()
