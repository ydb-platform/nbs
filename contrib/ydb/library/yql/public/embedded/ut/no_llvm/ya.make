UNITTEST()

TAG(ya:manual)

SIZE(MEDIUM)

TIMEOUT(300)

PEERDIR(
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/public/embedded/no_llvm
)

YQL_LAST_ABI_VERSION()

SRCDIR(
    contrib/ydb/library/yql/public/embedded/ut
)

SRCS(
    yql_embedded_ut.cpp
)

END()

