#pragma once

#include "public.h"

#include "node_registration_settings.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <cloud/storage/core/protos/config_dispatcher_settings.pb.h>

#include <contrib/ydb/core/config/init/init.h>
#include <contrib/ydb/core/protos/config.pb.h>
#include <contrib/ydb/core/protos/node_broker.pb.h>
#include <contrib/ydb/library/actors/core/defs.h>

#include <util/generic/map.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>

namespace NCloud::NStorage {

////////////////////////////////////////////////////////////////////////////////

// CMS response loaded during node registration. AppConfig holds the selected
// PROTO or YAML configuration; PrivateDatabaseConfig carries the caller's
// parsed PrivateDatabaseConfig section or its parsing error.
struct TCmsConfig
{
    // AppConfig selected according to UseYamlConfig and YAML YamlConfigEnabled.
    NKikimrConfig::TAppConfig AppConfig;

    // Parsed PrivateDatabaseConfig or NCloud::NProto::TError on failure.
    // Null if the section was not parsed. A successfully parsed empty section
    // holds a non-null message.
    std::shared_ptr<const google::protobuf::Message> PrivateDatabaseConfig;
};

////////////////////////////////////////////////////////////////////////////////

struct INodeRegistrant
{
    using TRegistrationResult = std::tuple<ui32, NActors::TScopeId>;

    virtual ~INodeRegistrant() = default;

    virtual TResultOrError<TRegistrationResult> RegisterNode(
        const TString& nodeBrokerAddress) = 0;

    virtual TResultOrError<TCmsConfig> GetConfigs(
        const TString& nodeBrokerAddress,
        ui32 nodeId) = 0;
};

using INodeRegistrantPtr = std::unique_ptr<INodeRegistrant>;

////////////////////////////////////////////////////////////////////////////////

struct TRegisterDynamicNodeOptions
{
    using TNodeLabels = TMap<TString, TString>;

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

    // YAML source selection: true selects the full YAML config when its
    // YamlConfigEnabled is true; false uses PROTO with
    // TAppConfig::BlockstoreConfig from YAML. Used only when LoadCmsConfigs
    // is true.
    bool UseYamlConfig = false;

    TNodeRegistrationSettings Settings;

    TNodeLabels Labels;

    // Parser for PrivateDatabaseConfigItem; empty disables its parsing.
    // Called only for a present section in the selected YAML configuration.
    NKikimr::NConfig::TOpaqueConfigParser PrivateDatabaseConfigParser;
};

////////////////////////////////////////////////////////////////////////////////

using TRegisterDynamicNodeResult =
    std::tuple<ui32, NActors::TScopeId, TMaybe<TCmsConfig>>;

INodeRegistrantPtr CreateNodeRegistrant(
    NKikimrConfig::TAppConfigPtr appConfig,
    const TRegisterDynamicNodeOptions& options,
    TLog& Log);

TRegisterDynamicNodeResult RegisterDynamicNode(
    NKikimrConfig::TAppConfigPtr appConfig,
    const TRegisterDynamicNodeOptions& options,
    INodeRegistrantPtr registrant,
    TLog& Log,
    ITimerPtr timer = CreateWallClockTimer());

}   // namespace NCloud::NStorage
