UNITTEST_FOR(contrib/ydb/library/yql/providers/yt/codec)

TAG(ya:manual)

SRCS(
    yt_codec_io_ut.cpp
)

PEERDIR(
    library/cpp/yson/node
    contrib/ydb/library/yql/minikql/computation/llvm14
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/sql
    contrib/ydb/library/yql/sql/pg_dummy
    contrib/ydb/library/yql/providers/common/codec
    contrib/ydb/library/yql/providers/common/mkql
    contrib/ydb/library/yql/providers/yt/lib/yson_helpers
    contrib/ydb/library/yql/providers/yt/codec/codegen
)

YQL_LAST_ABI_VERSION()

END()
