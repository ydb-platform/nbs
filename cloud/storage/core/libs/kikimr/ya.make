LIBRARY()

SRCS(
    actorsystem.cpp
    components_start.cpp
    components.cpp
    config_initializer.cpp
    events.cpp
    helpers.cpp
    node.cpp
    options.cpp
    proxy.cpp
    tenant.cpp
)

PEERDIR(
    cloud/storage/core/libs/actors
    cloud/storage/core/libs/common
    cloud/storage/core/libs/diagnostics

    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/util
    contrib/ydb/library/actors/wilson
    library/cpp/getopt/small
    library/cpp/lwtrace

    contrib/ydb/core/base
    contrib/ydb/core/mind
    contrib/ydb/core/protos
    contrib/ydb/core/tx/coordinator
    contrib/ydb/core/tx/mediator
    contrib/ydb/core/tx/schemeshard
)

END()
