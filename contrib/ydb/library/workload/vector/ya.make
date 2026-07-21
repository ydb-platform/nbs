LIBRARY()

SRCS(
    configure_opts.cpp
    vector_command_index.cpp
    vector_data_generator.cpp
    vector_recall_evaluator.cpp
    vector_sampler.cpp
    vector_sql.cpp
    vector_workload_generator.cpp
    vector_workload_params.cpp
    vector.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    library/cpp/colorizer
    contrib/ydb/library/formats/arrow/csv/converter
    contrib/ydb/library/workload/abstract
    contrib/ydb/library/workload/benchmark_base
    contrib/ydb/public/api/protos
    contrib/ydb/public/sdk/cpp/src/client/types/status
)

GENERATE_ENUM_SERIALIZATION_WITH_HEADER(vector_enums.h)

END()
