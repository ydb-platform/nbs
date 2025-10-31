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

    ydb/library/actors/core
    library/cpp/deprecated/atomic
    library/cpp/lwtrace

    ydb/core/base
    ydb/core/engine/minikql
    ydb/core/protos
    ydb/core/tablet
    ydb/core/tablet_flat
    ydb/library/yql/sql/pg_dummy
)

END()

RECURSE_FOR_TESTS(
   ut
)
