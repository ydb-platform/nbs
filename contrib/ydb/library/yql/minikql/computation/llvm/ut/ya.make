UNITTEST()

PEERDIR(
    contrib/ydb/library/yql/minikql/codegen/llvm
    contrib/ydb/library/yql/minikql/computation/llvm
    contrib/ydb/library/yql/minikql/comp_nodes/llvm
    contrib/libs/llvm12/lib/IR
    contrib/libs/llvm12/lib/ExecutionEngine/MCJIT
    contrib/libs/llvm12/lib/Linker
    contrib/libs/llvm12/lib/Target/X86
    contrib/libs/llvm12/lib/Target/X86/AsmParser
    contrib/libs/llvm12/lib/Transforms/IPO
)

INCLUDE(../../ut/ya.make.inc)

END()
