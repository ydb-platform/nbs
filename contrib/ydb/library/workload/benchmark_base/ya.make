LIBRARY()

SRCS(
    data_generator.cpp
    workload.cpp
    state.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    library/cpp/streams/factory/open_by_signature
    contrib/ydb/library/accessor
    contrib/ydb/library/formats/arrow/validation
    contrib/ydb/library/workload/abstract
    contrib/ydb/library/yaml_json
    contrib/ydb/public/api/protos
)

GENERATE_ENUM_SERIALIZATION_WITH_HEADER(workload.h)

END()

RECURSE_FOR_TESTS(
    ut
)
