UNITTEST()

TAG(ya:manual)

SIZE(MEDIUM)

TIMEOUT(300)

PEERDIR(
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/public/purecalc
    contrib/ydb/library/yql/public/purecalc/io_specs/mkql
    contrib/ydb/library/yql/public/purecalc/ut/lib
)

YQL_LAST_ABI_VERSION()

SRCS(
    test_spec.cpp
)

END()
