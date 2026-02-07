#pragma once
#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/public/sdk/cpp/client/ydb_driver/driver.h>
#include <contrib/ydb/public/sdk/cpp/client/ydb_common_client/settings.h>

namespace NPGW {

NActors::IActor* CreateConnection(NYdb::TDriver driver, std::unordered_map<TString, TString> params);

}
