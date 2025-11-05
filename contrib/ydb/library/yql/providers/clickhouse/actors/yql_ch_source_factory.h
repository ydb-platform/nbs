#pragma once

#include <contrib/ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>
#include <contrib/ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>

#include <contrib/ydb/library/yql/providers/common/http_gateway/yql_http_gateway.h>

#include <contrib/ydb/library/yql/providers/common/token_accessor/client/factory.h>

namespace NYql::NDq {

void RegisterClickHouseReadActorFactory(TDqAsyncIoFactory& factory, ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory, IHTTPGateway::TPtr gateway = IHTTPGateway::Make());

}
