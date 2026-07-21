#pragma once
#include <contrib/ydb/library/yql/minikql/computation/mkql_computation_node.h>

namespace NKikimr {
namespace NMiniKQL {

IComputationNode* WrapHostRuntimeSetting(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapUdfRuntimeSetting(TCallable& callable, const TComputationNodeFactoryContext& ctx);

} // namespace NMiniKQL
} // namespace NKikimr
