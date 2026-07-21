LIBRARY()

SRCS(
    yt_arrow_converter.cpp
    yt_arrow_output_converter.cpp
    yt_codec_io.cpp
    yt_codec_io.h
    yt_codec_job.cpp
    yt_codec_job.h
    yt_codec_tz.h
    yt_codec.cpp
    yt_codec.h
)

PEERDIR(
    library/cpp/streams/brotli
    library/cpp/yson
    library/cpp/yson/node
    yt/cpp/mapreduce/interface
    yt/cpp/mapreduce/io
    contrib/libs/apache/arrow
    contrib/ydb/library/yql/core
    contrib/ydb/library/yql/minikql
    contrib/ydb/library/yql/minikql/computation
    contrib/ydb/library/yql/public/udf
    contrib/ydb/library/yql/utils
    contrib/ydb/library/yql/parser/pg_wrapper/interface
    contrib/ydb/library/yql/public/result_format
    contrib/ydb/library/yql/public/udf/arrow
    contrib/ydb/library/yql/providers/common/codec
    contrib/ydb/library/yql/providers/common/codec/arrow
    contrib/ydb/library/yql/providers/common/schema/mkql
    contrib/ydb/library/yql/providers/common/schema/parser
    contrib/ydb/library/yql/providers/yt/common
    contrib/ydb/library/yql/providers/yt/lib/mkql_helpers
    contrib/ydb/library/yql/providers/yt/lib/skiff
    yt/yt/library/decimal
    contrib/ydb/library/yql/providers/common/codec/yt_arrow_converter_interface
)

IF (MKQL_DISABLE_CODEGEN)
    CFLAGS(
        -DMKQL_DISABLE_CODEGEN
    )
ENDIF()

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    codegen
)

RECURSE_FOR_TESTS(
    ut
    ut/no_llvm
)
