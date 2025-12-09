LIBRARY()

SRCS(
    yql_testlib.cpp
    yql_testlib.h
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/src/library/grpc/client
    library/cpp/regex/pcre
    library/cpp/testing/unittest
    contrib/ydb/core/base
    contrib/ydb/core/client
    contrib/ydb/core/client/minikql_compile
    contrib/ydb/core/client/server
    contrib/ydb/core/engine
    contrib/ydb/core/keyvalue
    contrib/ydb/core/mind
    contrib/ydb/core/protos
    contrib/ydb/core/testlib/default
    contrib/ydb/core/testlib/actors
    contrib/ydb/core/testlib/basics
    yql/essentials/core/facade
    yql/essentials/public/udf
    contrib/ydb/public/lib/base
    yql/essentials/core
    yql/essentials/providers/common/provider
    yql/essentials/providers/common/udf_resolve
)

YQL_LAST_ABI_VERSION()

END()
