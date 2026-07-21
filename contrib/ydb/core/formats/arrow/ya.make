LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/core/formats/arrow/serializer
    contrib/ydb/core/kqp/common/result_set_format
    contrib/ydb/core/scheme
    contrib/ydb/library/actors/core
    contrib/ydb/library/formats/arrow
    contrib/ydb/library/services
    contrib/ydb/library/yql/minikql
    contrib/ydb/library/yql/types/binary_json
    contrib/ydb/library/yql/types/dynumber
)

YQL_LAST_ABI_VERSION()

SRCS(
    arrow_batch_builder.cpp
    arrow_helpers.cpp
    arrow_helpers_minikql.cpp
    converter.cpp
    converter.h
    permutations.cpp
    process_columns.cpp
    size_calcer.cpp
    special_keys.cpp
)

END()

RECURSE(
    accessor
    container
    dictionary
    filter
    hash
    printer
    reader
    rows
    save_load
    splitter
    transformer
)

RECURSE_FOR_TESTS(
    ut
)
