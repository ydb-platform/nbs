#pragma once

#include <contrib/ydb/library/yql/minikql/comp_nodes/mkql_factories.h>

namespace NKikimr::NMiniKQL {

TComputationNodeFactory GetDqNodeFactory(TComputationNodeFactory customFactory = {});

} // namespace NKikimr::NMiniKQL
