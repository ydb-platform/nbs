#pragma once

#include "node_registration_settings.h"
#include "public.h"

#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <contrib/ydb/core/protos/config.pb.h>
#include <contrib/ydb/core/protos/node_broker.pb.h>
#include <contrib/ydb/library/actors/core/defs.h>

#include <util/generic/maybe.h>
#include <util/generic/string.h>

namespace NCloud::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TRegisterDynamicNodeOptions
{
    TString Domain;
    TString SchemeShardDir;

    TString NodeBrokerAddress;
    ui32 NodeBrokerPort = 0;
    ui32 NodeBrokerSecurePort = 0;
    bool UseNodeBrokerSsl = false;

    ui32 InterconnectPort = 0;

    TString DataCenter;
    TString Rack;
    ui64 Body = 0;

    bool LoadCmsConfigs = false;

    TNodeRegistrationSettings Settings;
};

////////////////////////////////////////////////////////////////////////////////

using TRegisterDynamicNodeResult =
    std::tuple<ui32, NActors::TScopeId, TMaybe<NKikimrConfig::TAppConfig>>;

////////////////////////////////////////////////////////////////////////////////

TRegisterDynamicNodeResult RegisterDynamicNode(
    NKikimrConfig::TAppConfigPtr appConfig,
    const TRegisterDynamicNodeOptions& options,
    TLog& Log);

}   // namespace NCloud::NStorage
