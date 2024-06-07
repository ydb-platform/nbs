LIBRARY()

OWNER(
    g:yql
    g:yql_ydb_core
)

NO_COMPILER_WARNINGS()

PEERDIR(
    contrib/ydb/library/yql/minikql/codegen/llvm
    contrib/ydb/library/yql/minikql/invoke_builtins/llvm14
    contrib/libs/llvm12/lib/IR
    contrib/libs/llvm12/lib/ExecutionEngine/MCJIT
    contrib/libs/llvm12/lib/Linker
    contrib/libs/llvm12/lib/Target/X86
    contrib/libs/llvm12/lib/Target/X86/AsmParser
    contrib/libs/llvm12/lib/Transforms/IPO
)

INCLUDE(../ya.make.inc)

END()
