#pragma once
#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/public/sdk/cpp/client/ydb_driver/driver.h>

namespace NPGW {

NActors::IActor* CreateDatabaseProxy(const NYdb::TDriverConfig& driverConfig);

}
