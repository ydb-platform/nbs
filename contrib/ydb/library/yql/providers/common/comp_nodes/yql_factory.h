#pragma once
#include <contrib/ydb/library/yql/minikql/computation/mkql_computation_node.h>

namespace NKikimr::NMiniKQL {

TComputationNodeFactory GetYqlFactory(ui32 exprCtxMutableIndex);
TComputationNodeFactory GetYqlFactory();

} // namespace NKikimr::NMiniKQL
