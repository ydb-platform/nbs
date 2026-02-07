RECURSE_FOR_TESTS(
    ut
)

LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/core/scheme
    contrib/ydb/core/formats/arrow/accessor
    contrib/ydb/core/formats/arrow/serializer
    contrib/ydb/core/formats/arrow/dictionary
    contrib/ydb/core/formats/arrow/transformer
    contrib/ydb/core/formats/arrow/reader
    contrib/ydb/core/formats/arrow/save_load
    contrib/ydb/core/formats/arrow/splitter
    contrib/ydb/core/formats/arrow/hash
    contrib/ydb/library/actors/core
    contrib/ydb/library/arrow_kernels
    contrib/ydb/library/binary_json
    contrib/ydb/library/dynumber
    contrib/ydb/library/formats/arrow
    contrib/ydb/library/services
    contrib/ydb/library/yql/core/arrow_kernels/request
)

IF (OS_WINDOWS)
    ADDINCL(
        contrib/ydb/library/yql/udfs/common/clickhouse/client/base
        contrib/ydb/library/arrow_clickhouse
    )
ELSE()
    PEERDIR(
        contrib/ydb/library/arrow_clickhouse
    )
    ADDINCL(
        contrib/ydb/library/arrow_clickhouse
    )
ENDIF()

YQL_LAST_ABI_VERSION()

SRCS(
    arrow_batch_builder.cpp
    arrow_filter.cpp
    arrow_helpers.cpp
    converter.cpp
    converter.h
    custom_registry.cpp
    permutations.cpp
    program.cpp
    size_calcer.cpp
    ssa_program_optimizer.cpp
    special_keys.cpp
    process_columns.cpp
)

END()
