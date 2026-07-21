G_BENCHMARK()

TAG(ya:fat)
SIZE(LARGE)
TIMEOUT(600)

IF (BENCHMARK_MAKE_LARGE_PART)
    CFLAGS(
        -DBENCHMARK_MAKE_LARGE_PART=1
    )
    TIMEOUT(1200)
ENDIF()

SRCS(
    write.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    contrib/ydb/library/yql/types/binary_json
    contrib/ydb/library/yql/minikql/dom
    contrib/ydb/library/yql/minikql/invoke_builtins/llvm16
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/public/issue/protos
    contrib/ydb/library/yql/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()


END()
