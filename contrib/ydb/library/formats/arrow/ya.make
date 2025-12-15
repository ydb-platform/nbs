RECURSE_FOR_TESTS(
    ut
)

LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/library/formats/arrow/accessor
    contrib/ydb/library/formats/arrow/simple_builder
    contrib/ydb/library/formats/arrow/transformer
    contrib/ydb/library/formats/arrow/splitter
    contrib/ydb/library/formats/arrow/modifier
    contrib/ydb/library/formats/arrow/scalar
    contrib/ydb/library/formats/arrow/hash
    contrib/ydb/library/actors/core
    contrib/ydb/library/arrow_kernels
    contrib/ydb/library/binary_json
    contrib/ydb/library/dynumber
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
    arrow_helpers.cpp
    input_stream.h
    permutations.cpp
    replace_key.cpp
    size_calcer.cpp
    simple_arrays_cache.cpp
)

END()
