LIBRARY(filestore-libs-storage-core)

SRCS(
    config.cpp
    helpers.cpp
    model.cpp
    probes.cpp
    request_info.cpp
    tablet.cpp
    tablet_counters.cpp
    tablet_schema.cpp
)

PEERDIR(
    cloud/filestore/config
    cloud/filestore/libs/service
    cloud/filestore/public/api/protos

    cloud/storage/core/libs/common

    contrib/ydb/library/actors/core
    library/cpp/deprecated/atomic
    library/cpp/lwtrace

    contrib/ydb/core/base
    contrib/ydb/core/engine/minikql
    contrib/ydb/core/protos
    contrib/ydb/core/tablet
    contrib/ydb/core/tablet_flat
    yql/essentials/sql/pg_dummy
)

END()

RECURSE_FOR_TESTS(
   ut
)
