#pragma once

#include <ydb-cpp-sdk/client/driver/driver.h>

#include <contrib/ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>
#include <contrib/ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>

#include <contrib/ydb/library/yql/providers/common/token_accessor/client/factory.h>

namespace NYql::NDq {

void RegisterYdbReadActorFactory(NYql::NDq::TDqAsyncIoFactory& factory, NYdb::TDriver driver, ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory);

}
