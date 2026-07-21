UNITTEST()

SRCS(
    yql_yt_native_folders_ut.cpp
)

PEERDIR(
    contrib/ydb/library/yql/providers/yt/gateway/native
    contrib/ydb/library/yql/providers/yt/gateway/file
    contrib/ydb/library/yql/providers/yt/codec/codegen
    contrib/ydb/library/yql/providers/yt/comp_nodes/llvm16
    contrib/ydb/library/yql/providers/yt/lib/access_provider/dummy
    contrib/ydb/library/yql/providers/yt/lib/secret_masker/dummy
    contrib/ydb/library/yql/providers/yt/lib/tvm_client/dummy
    contrib/ydb/library/yql/providers/yt/lib/ut_common
    library/cpp/testing/mock_server
    library/cpp/testing/common
    contrib/ydb/library/yql/public/udf/service/terminate_policy
    contrib/ydb/library/yql/sql/pg
    contrib/ydb/library/yql/minikql/comp_nodes/llvm16
    contrib/ydb/library/yql/minikql/invoke_builtins/llvm16
)

YQL_LAST_ABI_VERSION()

END()
