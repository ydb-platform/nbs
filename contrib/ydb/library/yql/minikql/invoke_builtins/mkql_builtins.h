#pragma once

#include <contrib/ydb/library/yql/minikql/mkql_function_metadata.h>

namespace NKikimr::NMiniKQL {

IBuiltinFunctionRegistry::TPtr CreateBuiltinRegistry();

} // namespace NKikimr::NMiniKQL
