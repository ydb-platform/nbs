#pragma once
#include <contrib/ydb/library/yql/minikql/computation/mkql_computation_node.h>

namespace NKikimr::NMiniKQL {

TComputationNodeFactory GetBuiltinFactory();

TComputationNodeFactory GetCompositeWithBuiltinFactory(TVector<TComputationNodeFactory> factories);

} // namespace NKikimr::NMiniKQL
