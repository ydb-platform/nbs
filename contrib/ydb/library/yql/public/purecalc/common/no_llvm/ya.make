LIBRARY()

INCLUDE(../ya.make.inc)

PEERDIR(
    contrib/ydb/library/yql/providers/yt/codec/codegen/no_llvm
    contrib/ydb/library/yql/providers/config
    contrib/ydb/library/yql/minikql/computation/no_llvm
    contrib/ydb/library/yql/minikql/invoke_builtins/no_llvm
    contrib/ydb/library/yql/minikql/comp_nodes/no_llvm
    contrib/ydb/library/yql/minikql/codegen/no_llvm
    contrib/ydb/library/yql/parser/pg_wrapper
    contrib/ydb/library/yql/parser/pg_wrapper/interface
)

END()

