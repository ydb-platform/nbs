SRCDIR(
    contrib/ydb/library/yql/public/purecalc/io_specs/mkql
)

ADDINCL(
    contrib/ydb/library/yql/public/purecalc/io_specs/mkql
)

PEERDIR(
    contrib/ydb/library/yql/providers/yt/codec
    contrib/ydb/library/yql/providers/yt/common
    contrib/ydb/library/yql/providers/yt/lib/mkql_helpers
    contrib/ydb/library/yql/providers/common/codec
    contrib/ydb/library/yql/providers/common/schema/mkql
)


YQL_LAST_ABI_VERSION()


SRCS(
    spec.cpp
    spec.h
)

