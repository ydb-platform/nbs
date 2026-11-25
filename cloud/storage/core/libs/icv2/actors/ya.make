LIBRARY()

SRCS(
    address_resolver.cpp
)

PEERDIR(
    cloud/storage/core/libs/actors
    cloud/storage/core/libs/common
    cloud/storage/core/libs/icv2/tcp
    cloud/storage/core/libs/kikimr

    contrib/ydb/library/actors/core
)

END()
