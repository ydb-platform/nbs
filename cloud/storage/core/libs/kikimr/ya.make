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

    ydb/library/actors/core
    ydb/library/actors/util
    ydb/library/actors/protos
    ydb/library/actors/wilson

    ydb/core/base
    ydb/core/config/init
    ydb/core/mind
    ydb/core/protos
    ydb/core/tx/coordinator
    ydb/core/tx/mediator
    ydb/core/tx/schemeshard
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(ut)
