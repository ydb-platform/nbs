#include <contrib/ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <contrib/ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <contrib/ydb/library/yql/minikql/mkql_function_registry.h>
#include <contrib/ydb/library/yql/minikql/mkql_program_builder.h>
#include <concepts>
namespace NKikimr::NMiniKQL {

struct TScalarLayoutConverterTestData {
    TScalarLayoutConverterTestData()
        : FunctionRegistry(NMiniKQL::CreateFunctionRegistry(NMiniKQL::CreateBuiltinRegistry()))
        , Alloc(__LOCATION__)
        , Env(Alloc)
        , PgmBuilder(Env, *FunctionRegistry)
        , MemInfo("Memory")
        , HolderFactory(Alloc.Ref(), MemInfo, FunctionRegistry.Get())
    {
    }

    TIntrusivePtr<NMiniKQL::IFunctionRegistry> FunctionRegistry;
    NMiniKQL::TScopedAlloc Alloc;
    NMiniKQL::TTypeEnvironment Env;
    NMiniKQL::TProgramBuilder PgmBuilder;
    NMiniKQL::TMemoryUsageInfo MemInfo;
    NMiniKQL::THolderFactory HolderFactory;
};


}