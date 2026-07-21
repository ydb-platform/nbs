LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/util
    contrib/ydb/library/formats/arrow
    contrib/ydb/library/formats/arrow/hash
    contrib/ydb/library/mkql_proto
    contrib/ydb/library/yql/dq/actors/compute/events
    contrib/ydb/library/yql/dq/actors/protos
    contrib/ydb/library/yql/dq/common
    contrib/ydb/library/yql/dq/comp_nodes
    contrib/ydb/library/yql/dq/expr_nodes
    contrib/ydb/library/yql/dq/runtime/streaming
    contrib/ydb/library/yql/dq/type_ann
    contrib/ydb/library/yverify_stream
    contrib/ydb/library/yql/minikql
    contrib/ydb/library/yql/minikql/arrow
    contrib/ydb/library/yql/minikql/comp_nodes
    contrib/ydb/library/yql/minikql/computation
    contrib/ydb/library/yql/parser/pg_wrapper/interface
    contrib/ydb/library/yql/providers/common/comp_nodes
    contrib/ydb/library/yql/providers/common/schema/mkql
    contrib/ydb/library/yql/public/udf
)

SRCS(
    dq_arrow_helpers.cpp
    dq_async_input.cpp
    dq_async_output.cpp
    dq_channel_service.cpp
    dq_channel_service_pack.cpp
    dq_columns_resolve.cpp
    dq_compute.cpp
    dq_input_channel.cpp
    dq_input_producer.cpp
    dq_output_channel.cpp
    dq_output_consumer.cpp
    dq_tasks_counters.cpp
    dq_tasks_runner.cpp
    dq_transport.cpp
)

GENERATE_ENUM_SERIALIZATION(dq_tasks_runner.h)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
