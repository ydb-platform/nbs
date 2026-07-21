#pragma once
#include <contrib/ydb/library/yql/core/yql_data_provider.h>
#include <contrib/ydb/library/yql/minikql/mkql_function_registry.h>

namespace NYql {

struct TTypeAnnotationContext;

struct TPureState: public TThrRefBase {
    using TPtr = TIntrusivePtr<TPureState>;

    TTypeAnnotationContext* Types;
    const NKikimr::NMiniKQL::IFunctionRegistry* FunctionRegistry;
};

TIntrusivePtr<IDataProvider> CreatePureProvider(const TPureState::TPtr& state);

TDataProviderInitializer GetPureDataProviderInitializer();

} // namespace NYql
