PY23_NATIVE_LIBRARY()

YQL_ABI_VERSION(2 27 0)

SRCS(
    python_udf.cpp
)

PEERDIR(
    contrib/ydb/library/yql/public/udf
    contrib/ydb/library/yql/udfs/common/python/bindings
)

CFLAGS(
    -DDISABLE_PYDEBUG
)

NO_COMPILER_WARNINGS()

END()
