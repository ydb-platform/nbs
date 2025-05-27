#pragma once

#include "public.h"

#include "node_registration_settings.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/common/sleeper.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <contrib/ydb/core/protos/config.pb.h>
#include <contrib/ydb/core/protos/node_broker.pb.h>
#include <contrib/ydb/library/actors/core/defs.h>

#include <util/generic/maybe.h>
#include <util/generic/string.h>

namespace NCloud::NStorage {

////////////////////////////////////////////////////////////////////////////////

using TRegistrationResult = std::tuple<ui32, NActors::TScopeId>;

////////////////////////////////////////////////////////////////////////////////

struct INodeRegistrant
{
    virtual ~INodeRegistrant() = default;

    virtual TResultOrError<TRegistrationResult> TryRegister(
        const TString& nodeBrokerAddress) = 0;

    virtual TResultOrError<NKikimrConfig::TAppConfig> TryConfigure(
        const TString& nodeBrokerAddress,
        ui32 nodeId) = 0;
};

using INodeRegistrantPtr = std::unique_ptr<INodeRegistrant>;

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

INodeRegistrantPtr CreateNodeRegistrant(
    NKikimrConfig::TAppConfigPtr appConfig,
    const TRegisterDynamicNodeOptions& options,
    TLog& Log);

TRegisterDynamicNodeResult RegisterDynamicNode(
    NKikimrConfig::TAppConfigPtr appConfig,
    const TRegisterDynamicNodeOptions& options,
    INodeRegistrantPtr registrant,
    TLog& Log,
    ITimerPtr timer = CreateWallClockTimer(),
    ISleeperPtr sleeper = CreateDefaultSleeper());

}   // namespace NCloud::NStorage
