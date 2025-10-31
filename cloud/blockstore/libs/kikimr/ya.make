LIBRARY()

SRCS(
    components.cpp
    events.cpp
    helpers.cpp
)

PEERDIR(
    cloud/blockstore/libs/common
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/service
    cloud/blockstore/public/api/protos
    cloud/storage/core/libs/kikimr
    ydb/library/actors/core
    ydb/library/yql/public/udf
    ydb/library/yql/public/udf/service/exception_policy
    ydb/library/yql/sql/pg_dummy
    library/cpp/lwtrace
    ydb/core/base
    ydb/core/protos
)

END()
