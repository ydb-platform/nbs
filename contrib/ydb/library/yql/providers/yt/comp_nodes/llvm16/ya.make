LIBRARY()

NO_COMPILER_WARNINGS()

PEERDIR(
    contrib/ydb/library/yql/minikql/codegen/llvm16
    contrib/ydb/library/yql/minikql/invoke_builtins/llvm16
    contrib/ydb/library/yql/providers/yt/codec/codegen/llvm16
)

INCLUDE(../ya.make.inc)

END()
