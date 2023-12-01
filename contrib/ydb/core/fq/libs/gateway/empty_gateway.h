#pragma once
#include <contrib/ydb/library/yql/providers/dq/provider/yql_dq_gateway.h>

#include <contrib/ydb/library/actors/core/actorsystem.h>

namespace NFq {

TIntrusivePtr<NYql::IDqGateway> CreateEmptyGateway(NActors::TActorId runActorId);

} // namespace NFq
