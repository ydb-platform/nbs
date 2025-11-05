UNITTEST_FOR(contrib/ydb/library/yql/providers/yt/actors)

TAG(ya:manual)

PEERDIR(
    contrib/ydb/library/yql/providers/yt/codec/codegen/no_llvm
    contrib/ydb/library/yql/providers/yt/comp_nodes/no_llvm
    contrib/ydb/library/yql/providers/yt/gateway/file
    contrib/ydb/library/yql/minikql/codegen/no_llvm
    contrib/ydb/library/actors/testlib
    contrib/ydb/library/yql/public/udf
    library/cpp/testing/unittest
    contrib/ydb/library/yql/sql/pg
    contrib/ydb/library/yql/public/udf/service/terminate_policy
    contrib/ydb/library/yql/parser/pg_wrapper/interface

)

SRCS(
    yql_yt_lookup_actor_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()

