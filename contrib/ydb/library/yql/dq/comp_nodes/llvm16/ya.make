LIBRARY()

INCLUDE(../ya.make.inc)

PEERDIR(
    contrib/ydb/library/yql/minikql/comp_nodes/llvm16
    contrib/ydb/library/yql/minikql/computation/llvm16
    contrib/ydb/library/yql/minikql/codegen/llvm16
    contrib/ydb/library/yql/minikql/invoke_builtins/llvm16
)

END()
