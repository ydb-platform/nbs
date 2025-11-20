UNITTEST()

TAG(ya:manual)

SIZE(MEDIUM)

TIMEOUT(300)

PEERDIR(
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/public/purecalc/no_llvm
    contrib/ydb/library/yql/public/purecalc/io_specs/mkql/no_llvm
    contrib/ydb/library/yql/public/purecalc/ut/lib
)

YQL_LAST_ABI_VERSION()

SRCDIR(
   contrib/ydb/library/yql/public/purecalc/io_specs/mkql/ut
)

SRCS(
    test_spec.cpp
)

END()
