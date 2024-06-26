#pragma once

#include <contrib/ydb/library/yql/minikql/computation/mkql_computation_node.h>
#include <contrib/ydb/library/yql/minikql/mkql_stats_registry.h>

namespace NYql {

NKikimr::NMiniKQL::TComputationNodeFactory GetDqYtFactory(NKikimr::NMiniKQL::IStatsRegistry* jobStats = nullptr);

}
