UNITTEST_FOR(contrib/ydb/library/yql/providers/yt/actors)

PEERDIR(
    yt/yql/providers/yt/codec/codegen/no_llvm
    yt/yql/providers/yt/comp_nodes/no_llvm
    yt/yql/providers/yt/gateway/file
    yql/essentials/minikql/codegen/no_llvm
    contrib/ydb/library/actors/testlib
    yql/essentials/public/udf
    library/cpp/testing/unittest
    yql/essentials/sql/pg
    yql/essentials/public/udf/service/terminate_policy
    yql/essentials/parser/pg_wrapper/interface

)

SRCS(
    yql_yt_lookup_actor_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()

