/*******************************************************************************

Each accepted private_database_config replaces the previous YAML source and
is applied to the local configuration saved before CMS.
Parser failures preserve the published configuration. An absent section
restores that local configuration.

*******************************************************************************/

#include "configs_manager.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/critical_events.h>

#include <contrib/ydb/core/base/appdata.h>
#include <contrib/ydb/core/cms/console/configs_dispatcher.h>
#include <contrib/ydb/core/cms/console/console.h>
#include <contrib/ydb/core/protos/console_config.pb.h>
#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/yaml_config/yaml_config_helpers.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/string/builder.h>

namespace NCloud::NBlockStore {

using namespace NActors;
using namespace NKikimr::NConsole;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 PrivateDatabaseConfigKind =
    NKikimrConsole::TConfigItem::PrivateDatabaseConfigItem;

// ConfigsManager actor for private_database_config notifications. It owns the
// local config saved before CMS and last accepted YAML source, and publishes
// their combined config through the holder. Create it with CreateConfigsManager().
class TConfigsManagerActor final
    : public TActorBootstrapped<TConfigsManagerActor>
{
public:
    explicit TConfigsManagerActor(TConfigsManagerArgs args)
        : BlockstoreConfigHolder(std::move(args.BlockstoreConfigHolder))
        , StaticConfig(std::move(args.StaticConfig))
        , DynamicConfig(std::move(args.InitialDynamicConfig))
        , DynamicConfigPresent(args.InitialDynamicConfigPresent)
        , StorageConfigControls(std::move(args.StorageConfigControls))
        , ConfigsDispatcherId(args.ConfigsDispatcherId)
    {}

    void Bootstrap(const TActorContext& ctx)
    {
        RegisterCounters(ctx);

        ctx.Send(
            ConfigsDispatcherId
                ? ConfigsDispatcherId
                : MakeConfigsDispatcherID(ctx.SelfID.NodeId()),
            new TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest(
                PrivateDatabaseConfigKind,
                ctx.SelfID));

        Become(&TThis::StateWork);
    }

private:
    // Published Blockstore configuration; non-null and written only by this
    // actor after startup.
    const TBlockstoreConfigHolderPtr BlockstoreConfigHolder;

    // Local configuration saved before CMS, including CLI overrides.
    // Every update is applied to this unchanged configuration.
    const NProto::TBlockstoreConfig StaticConfig;

    // Last accepted private_database_config; empty after removal.
    NProto::TBlockstoreConfig DynamicConfig;

    // The presence flag distinguishing an absent section from a valid empty
    // dynamic message.
    bool DynamicConfigPresent;

    // ICB overrides shared by every published StorageConfig; non-null.
    const NStorage::TStorageConfigControlsPtr StorageConfigControls;

    // ConfigsDispatcher actor; zero selects the node-local service.
    const TActorId ConfigsDispatcherId;

    // The actor activity gauge: one after Bootstrap() completes.
    NMonitoring::TDynamicCounters::TCounterPtr Active;

    // The dynamic-section presence gauge: one while the section is present.
    NMonitoring::TDynamicCounters::TCounterPtr DynamicConfigPresentCounter;

    // The number of accepted non-duplicate updates, including removals.
    NMonitoring::TDynamicCounters::TCounterPtr SuccessfulUpdates;

    // The number of updates rejected before publication and acknowledgement.
    NMonitoring::TDynamicCounters::TCounterPtr RejectedUpdates;

    // The latest EConfigsManagerUpdateStatus value exposed as an integer.
    NMonitoring::TDynamicCounters::TCounterPtr LastUpdateStatus;

    void RegisterCounters(const TActorContext& ctx)
    {
        auto counters = NKikimr::AppData(ctx)->Counters
            ->GetSubgroup("counters", "blockstore")
            ->GetSubgroup("component", "configs_manager");

        Active = counters->GetCounter("Active", false);
        DynamicConfigPresentCounter =
            counters->GetCounter("DynamicConfigPresent", false);
        SuccessfulUpdates = counters->GetCounter("SuccessfulUpdates", true);
        RejectedUpdates = counters->GetCounter("RejectedUpdates", true);
        LastUpdateStatus = counters->GetCounter("LastUpdateStatus", false);

        *Active = 1;
        *DynamicConfigPresentCounter = DynamicConfigPresent;
        *LastUpdateStatus = static_cast<ui32>(
            EConfigsManagerUpdateStatus::Startup);
    }

    void Reject()
    {
        RejectedUpdates->Inc();
        *LastUpdateStatus = static_cast<ui32>(
            EConfigsManagerUpdateStatus::Rejected);
    }

    void Ack(
        const TEvConsole::TEvConfigNotificationRequest::TPtr& ev,
        const TActorContext& ctx)
    {
        ctx.Send(
            ev->Sender,
            new TEvConsole::TEvConfigNotificationResponse(ev->Get()->Record),
            0,
            ev->Cookie);
    }

    void HandleConfigNotification(
        const TEvConsole::TEvConfigNotificationRequest::TPtr& ev,
        const TActorContext& ctx)
    {
        const auto* message = ev->Get();
        NProto::TBlockstoreConfig dynamicConfig;
        bool dynamicConfigPresent = false;

        if (auto it = message->OpaqueConfigs.find(PrivateDatabaseConfigKind);
            it != message->OpaqueConfigs.end())
        {
            const auto payload = it->second;
            if (!payload) {
                Reject();
                return;
            }

            const auto* descriptor = payload->GetDescriptor();
            if (descriptor == NCloud::NProto::TError::descriptor()) {
                NCloud::NProto::TError error;
                error.CopyFrom(*payload);
                ReportGetConfigsFromCmsYamlParseError(
                    TStringBuilder()
                    << "Failed to parse private YAML configuration from CMS: "
                    << FormatError(error)
                    << ". Keeping the last successfully applied configuration. "
                       "Fix or roll back the cluster configuration before "
                       "restarting nodes: the previous dynamic configuration "
                       "is kept only in memory and will be lost after a restart.");
                Reject();
                return;
            }

            if (descriptor != NProto::TBlockstoreConfig::descriptor()) {
                Reject();
                return;
            }

            dynamicConfig.CopyFrom(*payload);
            RemoveStaticOnlyBlockstoreFields(&dynamicConfig);
            dynamicConfigPresent = true;
        }

        if (dynamicConfigPresent == DynamicConfigPresent &&
            dynamicConfig.SerializeAsString() == DynamicConfig.SerializeAsString())
        {
            Ack(ev, ctx);
            return;
        }

        try {
            const auto currentConfig = BlockstoreConfigHolder->Get();
            TBlockstoreConfigExtraParameters extraParameters;
            extraParameters.DiskAgent.Rack =
                currentConfig->GetDiskAgentConfig()->GetRack();
            extraParameters.DiskAgent.NetworkMbitThroughput =
                currentConfig->GetDiskAgentConfig()
                    ->GetNetworkMbitThroughput();
            auto config = MakeBlockstoreConfig(
                StaticConfig,
                dynamicConfig,
                StorageConfigControls,
                std::move(extraParameters));

            // Publish before acknowledgement so observers never see an ACK for
            // a configuration that is not yet available to readers.
            BlockstoreConfigHolder->Set(std::move(config));
            DynamicConfig = std::move(dynamicConfig);
            DynamicConfigPresent = dynamicConfigPresent;

            *DynamicConfigPresentCounter = DynamicConfigPresent;
            SuccessfulUpdates->Inc();
            *LastUpdateStatus = static_cast<ui32>(
                EConfigsManagerUpdateStatus::Applied);

            Ack(ev, ctx);
        } catch (...) {
            Reject();
        }
    }

    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(
                TEvConsole::TEvConfigNotificationRequest,
                HandleConfigNotification);
            IgnoreFunc(TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse);
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

NKikimr::NConfig::TOpaqueConfigParser CreateBlockstoreOpaqueConfigParser()
{
    return [] (const TString& yaml)
        -> std::shared_ptr<const google::protobuf::Message>
    {
        try {
            auto config = NKikimr::NYaml::DefaultOpaqueConfigParser<
                NProto::TBlockstoreConfig>(yaml, true);
            if (config) {
                return config;
            }
            return std::make_shared<NCloud::NProto::TError>(MakeError(
                E_ARGUMENT,
                "Empty private database config"));
        } catch (...) {
            return std::make_shared<NCloud::NProto::TError>(MakeError(
                E_ARGUMENT,
                "Failed to parse private database config"));
        }
    };
}

void RemoveStaticOnlyBlockstoreFields(NProto::TBlockstoreConfig* config)
{
    if (config->HasServer() && config->GetServer().HasServerConfig()) {
        config->MutableServer()
            ->MutableServerConfig()
            ->ClearDynamicYamlConfigurationEnabled();
    }
}

NActors::IActor* CreateConfigsManager(TConfigsManagerArgs args)
{
    return new TConfigsManagerActor(std::move(args));
}

}   // namespace NCloud::NBlockStore
