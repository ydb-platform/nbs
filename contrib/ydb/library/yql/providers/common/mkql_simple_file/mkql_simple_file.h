#pragma once

#include <contrib/ydb/library/yql/minikql/mkql_function_registry.h>
#include <contrib/ydb/library/yql/minikql/mkql_node_visitor.h>
#include <contrib/ydb/library/yql/core/yql_user_data.h>

namespace NYql {

class TSimpleFileTransformProvider {
public:
    TSimpleFileTransformProvider(const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
                                 const TUserDataTable& userDataBlocks);

    NKikimr::NMiniKQL::TCallableVisitFunc operator()(NKikimr::NMiniKQL::TInternName name);

protected:
    const NKikimr::NMiniKQL::IFunctionRegistry* FunctionRegistry_;
    const TUserDataTable& UserDataBlocks_;
};

} // namespace NYql
