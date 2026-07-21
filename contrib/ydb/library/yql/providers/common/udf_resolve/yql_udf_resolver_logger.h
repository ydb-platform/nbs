#pragma once

#include <contrib/ydb/library/yql/core/yql_udf_index.h>
#include <contrib/ydb/library/yql/core/file_storage/file_storage.h>
#include <contrib/ydb/library/yql/minikql/mkql_function_registry.h>

namespace NYql::NCommon {

IUdfResolver::TPtr CreateUdfResolverDecoratorWithLogger(const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry, IUdfResolver::TPtr underlying, const TString& path, const TString& sessionId);

} // namespace NYql::NCommon
