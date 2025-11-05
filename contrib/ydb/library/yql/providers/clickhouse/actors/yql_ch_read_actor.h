#pragma once

#include <contrib/ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>
#include <contrib/ydb/library/yql/providers/common/http_gateway/yql_http_gateway.h>
#include <contrib/ydb/library/yql/providers/clickhouse/proto/source.pb.h>
#include <contrib/ydb/library/yql/providers/common/token_accessor/client/factory.h>
#include <contrib/ydb/library/actors/core/actor.h>

namespace NYql::NDq {

std::pair<NYql::NDq::IDqComputeActorAsyncInput*, NActors::IActor*> CreateClickHouseReadActor(
    IHTTPGateway::TPtr gateway,
    NCH::TSource&& params,
    ui64 inputIndex,
    TCollectStatsLevel statsLevel,
    const THashMap<TString, TString>& secureParams,
    const THashMap<TString, TString>& taskParams,
    const NActors::TActorId& computeActorId,
    ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory);

} // namespace NYql::NDq
