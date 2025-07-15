LIBRARY()

SRCS(
    config_printer.cpp
    config_printer.h
)

PEERDIR(
    cloud/storage/core/libs/actors
    contrib/ydb/core/cms/console
    contrib/ydb/core/protos
    cloud/blockstore/libs/kikimr
    cloud/blockstore/libs/storage/api
    contrib/ydb/library/actors/core
)

END()
