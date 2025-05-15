LIBRARY()

SRCS(
    actorsystem.cpp
    components.cpp
    components_start.cpp
    config_dispatcher_helpers.cpp
    config_initializer.cpp
    events.cpp
    helpers.cpp
    kikimr_initializer.cpp
    node_registration_helpers.cpp
    node_registration_settings.cpp
    node.cpp
    options.cpp
    proxy.cpp
    tenant.cpp
)

PEERDIR(
    cloud/storage/core/libs/actors
    cloud/storage/core/libs/common
    cloud/storage/core/libs/daemon
    cloud/storage/core/libs/diagnostics
    cloud/storage/core/protos

    library/cpp/getopt/small
    library/cpp/lwtrace

    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/util
    contrib/ydb/library/actors/protos
    contrib/ydb/library/actors/wilson

    contrib/ydb/core/base
    contrib/ydb/core/config/init
    contrib/ydb/core/mind
    contrib/ydb/core/protos
    contrib/ydb/core/tx/coordinator
    contrib/ydb/core/tx/mediator
    contrib/ydb/core/tx/schemeshard
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(ut)
