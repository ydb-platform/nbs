LIBRARY()

CXXFLAGS(-DMKQL_DISABLE_CODEGEN)

INCLUDE(../ya.make.inc)

PEERDIR(
    contrib/ydb/library/yql/minikql/comp_nodes/no_llvm
    contrib/ydb/library/yql/minikql/computation/no_llvm
    contrib/ydb/library/yql/minikql/codegen/no_llvm
    contrib/ydb/library/yql/minikql/invoke_builtins/no_llvm
)

END()
