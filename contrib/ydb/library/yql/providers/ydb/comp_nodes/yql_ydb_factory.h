#pragma once

#include <contrib/ydb/library/yql/minikql/computation/mkql_computation_node.h>
#include <contrib/ydb/public/sdk/cpp/client/ydb_driver/driver.h>

namespace NYql {

NKikimr::NMiniKQL::TComputationNodeFactory GetDqYdbFactory(NYdb::TDriver driver);

}

