LIBRARY()

SRCS(
    actor_helpers.cpp
    actor_helpers.h
    common_helper.cpp
    cs_helper.cpp
    fake_coordinator.cpp
    fake_coordinator.h
    fake_scheme_shard.h
    minikql_compile.h
    mock_pq_metacache.h
    tablet_flat_dummy.cpp
    tablet_helpers.cpp
    tablet_helpers.h
    tenant_runtime.cpp
    tenant_runtime.h
    test_client.cpp
    test_client.h
    tx_helpers.cpp
    tx_helpers.h
)

PEERDIR(
    contrib/ydb/apps/version
    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/interconnect
    contrib/ydb/library/grpc/client
    contrib/ydb/library/grpc/server
    contrib/ydb/library/grpc/server/actors
    library/cpp/regex/pcre
    library/cpp/testing/gmock_in_unittest
    library/cpp/testing/unittest
    contrib/ydb/core/driver_lib/run
    contrib/ydb/core/base
    contrib/ydb/core/blobstorage/base
    contrib/ydb/core/blobstorage/pdisk
    contrib/ydb/core/client
    contrib/ydb/core/client/metadata
    contrib/ydb/core/client/minikql_compile
    contrib/ydb/core/client/server
    contrib/ydb/core/cms/console
    contrib/ydb/core/engine
    contrib/ydb/core/engine/minikql
    contrib/ydb/core/formats
    contrib/ydb/core/fq/libs/init
    contrib/ydb/core/fq/libs/mock
    contrib/ydb/core/fq/libs/shared_resources
    contrib/ydb/core/grpc_services
    contrib/ydb/core/health_check
    contrib/ydb/core/kesus/proxy
    contrib/ydb/core/kesus/tablet
    contrib/ydb/core/keyvalue
    contrib/ydb/core/kqp
    contrib/ydb/core/kqp/federated_query
    contrib/ydb/core/kqp/finalize_script_service
    contrib/ydb/core/metering
    contrib/ydb/core/mind
    contrib/ydb/core/mind/address_classification
    contrib/ydb/core/mind/bscontroller
    contrib/ydb/core/mind/hive
    contrib/ydb/core/node_whiteboard
    contrib/ydb/core/persqueue
    contrib/ydb/core/protos
    contrib/ydb/core/security
    contrib/ydb/core/statistics/aggregator
    contrib/ydb/core/sys_view/processor
    contrib/ydb/core/sys_view/service
    contrib/ydb/core/testlib/actors
    contrib/ydb/core/testlib/basics
    contrib/ydb/core/tx/columnshard
    contrib/ydb/core/tx/coordinator
    contrib/ydb/core/tx/long_tx_service
    contrib/ydb/core/tx/mediator
    contrib/ydb/core/tx/replication/controller
    contrib/ydb/core/tx/schemeshard
    contrib/ydb/core/tx/sequenceproxy
    contrib/ydb/core/tx/sequenceshard
    contrib/ydb/core/tx/time_cast
    contrib/ydb/library/aclib
    contrib/ydb/library/folder_service/mock
    contrib/ydb/library/mkql_proto/protos
    contrib/ydb/library/persqueue/topic_parser
    contrib/ydb/library/security
    contrib/ydb/library/yql/minikql/comp_nodes/llvm14
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/public/lib/base
    contrib/ydb/public/lib/deprecated/kicli
    contrib/ydb/public/sdk/cpp/client/ydb_driver
    contrib/ydb/public/sdk/cpp/client/ydb_persqueue_public/codecs
    contrib/ydb/public/sdk/cpp/client/ydb_query
    contrib/ydb/public/sdk/cpp/client/ydb_table
    contrib/ydb/services/auth
    contrib/ydb/services/cms
    contrib/ydb/services/datastreams
    contrib/ydb/services/discovery
    contrib/ydb/services/ext_index/service
    contrib/ydb/core/tx/conveyor/service
    contrib/ydb/services/fq
    contrib/ydb/services/kesus
    contrib/ydb/services/persqueue_cluster_discovery
    contrib/ydb/services/persqueue_v1
    contrib/ydb/services/rate_limiter
    contrib/ydb/services/monitoring
    contrib/ydb/services/metadata/ds_table
    contrib/ydb/services/bg_tasks/ds_table
    contrib/ydb/services/bg_tasks
    contrib/ydb/services/ydb

    contrib/ydb/core/http_proxy
)

YQL_LAST_ABI_VERSION()

IF (GCC)
    CFLAGS(
        -fno-devirtualize-speculatively
    )
ENDIF()

END()

RECURSE(
    actors
    basics
    default
    pg
)
