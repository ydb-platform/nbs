/*******************************************************************************

Each accepted PrivateDatabaseConfig replaces the previous YAML source and
is applied to the static configuration saved before CMS.
Parser failures preserve the published configuration. An empty or absent
section clears dynamic overrides; equivalent inputs do not republish.

*******************************************************************************/

#include "configs_manager.h"

#include "events.h"

#include <cloud/blockstore/libs/config/blockstore_config_management.h>
#include <cloud/blockstore/libs/config/opaque_config_parser.h>
#include <cloud/blockstore/libs/kikimr/components.h>

#include <cloud/storage/core/libs/actors/helpers.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/config/runtime_config.h>
#include <cloud/storage/core/libs/diagnostics/critical_events.h>

#include <contrib/ydb/core/cms/console/configs_dispatcher.h>
#include <contrib/ydb/core/cms/console/console.h>
#include <contrib/ydb/core/protos/console_config.pb.h>
#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/log.h>

#include <util/generic/hash_set.h>
#include <util/generic/yexception.h>
#include <util/string/builder.h>

#include <google/protobuf/util/message_differencer.h>

namespace NCloud::NBlockStore {

using namespace NActors;
using namespace NKikimr::NConsole;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 PrivateDatabaseConfigKind =
    NKikimrConsole::TConfigItem::PrivateDatabaseConfigItem;

// Report a rejected runtime update with its reason and the restart risk.
void ReportConfigUpdateError(const TString& reason, bool rollbackShallHelp)
{
    ReportDynamicConfigError(
        TStringBuilder()
        << reason
        << ". Keeping the last successfully applied configuration. "
           "The update is not accepted. "
        << (rollbackShallHelp
                ? ("Fix or roll back the cluster configuration before "
                   "restarting nodes: the previous correct dynamic "
                   "configuration is kept only in memory and will be lost "
                   "after a restart.")
                : ""));
}

class TConfigsManagerActor final
    : public TActorBootstrapped<TConfigsManagerActor>
{
public:
    explicit TConfigsManagerActor(TConfigsManagerArgs args);

    void Bootstrap(const TActorContext& ctx);

private:
    // Published Blockstore configuration; non-null and written only by this
    // actor after startup.
    const TBlockstoreConfigHolderPtr ConfigHolder;

    // Static configuration saved before CMS, including CLI overrides.
    // Every update is applied to this unchanged configuration.
    const NProto::TBlockstoreConfig StaticConfig;

    // Raw values fixed at node startup; owned independently of publications
    // and shared ICB controls, and never replaced by a runtime notification.
    const NProto::TBlockstoreConfig StartupConfig;

    // All rejected schema paths and reasons from the last accepted runtime
    // merge; parameter values and collection keys are excluded.
    NConfig::TRuntimeConfigDiagnostics RuntimeDiagnostics;

    // Last accepted PrivateDatabaseConfig before normalization or merging;
    // empty without a dynamic source.
    NProto::TBlockstoreConfig DynamicConfig;

    // ICB overrides shared by every published StorageConfig; non-null.
    const NStorage::TStorageConfigControlsPtr StorageConfigControls;

    // ConfigsDispatcher actor; zero selects the node-local service.
    const TActorId ConfigsDispatcherId;

    // Local recipients of publication notices; consumers remove their own
    // subscriptions before stopping.
    THashSet<TActorId> Subscribers;

    STFUNC(StateWork);

    // Register one recipient and send an initial notice on every request.
    void Handle(
        const TEvConfigsManager::TEvSetConfigSubscriptionRequest::TPtr& ev,
        const TActorContext& ctx);

    // Remove the selected recipient and confirm even an already absent entry.
    void Handle(
        const TEvConfigsManager::TEvRemoveConfigSubscriptionRequest::TPtr& ev,
        const TActorContext& ctx);

    // Remove a subscriber when delivery confirms that its actor no longer
    // exists.
    void Handle(const TEvents::TEvUndelivered::TPtr& ev);

    // Process new config from ConfigsDispatcher
    void Handle(
        const TEvConsole::TEvConfigNotificationRequest::TPtr& ev,
        const TActorContext& ctx);

    // Send ConfigNotificationResponse to ConfigsDispatcher
    void ReplyConfigNotificationResponse(
        const TEvConsole::TEvConfigNotificationRequest::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TConfigsManagerActor::TConfigsManagerActor(TConfigsManagerArgs args)
    : ConfigHolder(std::move(args.ConfigHolder))
    , StaticConfig(std::move(args.StaticConfig))
    , StartupConfig(std::move(args.StartupConfig))
    , DynamicConfig(std::move(args.InitialDynamicConfig))
    , StorageConfigControls(std::move(args.StorageConfigControls))
    , ConfigsDispatcherId(args.ConfigsDispatcherId)
{}

void TConfigsManagerActor::Bootstrap(const TActorContext& ctx)
{
    ctx.Send(
        ConfigsDispatcherId ? ConfigsDispatcherId
                            : MakeConfigsDispatcherID(ctx.SelfID.NodeId()),
        new TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest(
            PrivateDatabaseConfigKind,
            ctx.SelfID));

    Become(&TThis::StateWork);
}

STFUNC(TConfigsManagerActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvConfigsManager::TEvSetConfigSubscriptionRequest, Handle);
        HFunc(TEvConfigsManager::TEvRemoveConfigSubscriptionRequest, Handle);
        HFunc(TEvConsole::TEvConfigNotificationRequest, Handle);
        hFunc(TEvents::TEvUndelivered, Handle);
        IgnoreFunc(TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::CONFIGS_MANAGER,
                __PRETTY_FUNCTION__);
            break;
    }
}

// Register one recipient and send an initial notice on every request.
void TConfigsManagerActor::Handle(
    const TEvConfigsManager::TEvSetConfigSubscriptionRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto subscriber =
        ev->Get()->Subscriber ? ev->Get()->Subscriber : ev->Sender;
    Subscribers.insert(subscriber);

    // Confirm registration to the requester, then initialize the recipient
    // even when another actor requested the subscription on its behalf.
    ctx.Send(
        ev->Sender,
        new TEvConfigsManager::TEvSetConfigSubscriptionResponse(ConfigHolder),
        0,
        ev->Cookie);
    ctx.Send(
        subscriber,
        new TEvConfigsManager::TEvConfigChanged(ConfigHolder),
        IEventHandle::FlagTrackDelivery);
}

// Remove the selected recipient and confirm even an already absent entry.
void TConfigsManagerActor::Handle(
    const TEvConfigsManager::TEvRemoveConfigSubscriptionRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto subscriber =
        ev->Get()->Subscriber ? ev->Get()->Subscriber : ev->Sender;

    Subscribers.erase(subscriber);

    ctx.Send(
        ev->Sender,
        new TEvConfigsManager::TEvRemoveConfigSubscriptionResponse(),
        0,
        ev->Cookie);
}

// Remove a subscriber only when its config notice targets a missing actor.
void TConfigsManagerActor::Handle(const TEvents::TEvUndelivered::TPtr& ev)
{
    const auto* message = ev->Get();
    if (message->SourceType == TEvConfigsManager::TEvConfigChanged::EventType &&
        message->Reason == TEvents::TEvUndelivered::ReasonActorUnknown)
    {
        Subscribers.erase(ev->Sender);
    }
}

// Process new config from ConfigsDispatcher
void TConfigsManagerActor::Handle(
    const TEvConsole::TEvConfigNotificationRequest::TPtr& ev,
    const TActorContext& ctx)
{
    LOG_INFO_S(
        ctx,
        TBlockStoreComponents::CONFIGS_MANAGER,
        "Received YAML configuration from ConfigsDispatcher");

    const auto* message = ev->Get();
    NProto::TBlockstoreConfig newDynamicConfig;
    NConfig::TRuntimeConfigDiagnostics diagnostics;
    IBlockstoreConfigPtr newConfig;

    // Prepare the complete update before changing the publication or live
    // controls, so an exception leaves the last accepted state intact.
    try {
        if (auto it = message->OpaqueConfigs.find(PrivateDatabaseConfigKind);
            it != message->OpaqueConfigs.end())
        {
            const auto payload = it->second;
            if (!payload) {
                ReportConfigUpdateError(
                    "Internal error: received a null PrivateDatabaseConfig "
                    "payload from ConfigsDispatcher",
                    /*rollbackShallHelp=*/false);
                return;
            }

            auto [config, error] = ExtractBlockstoreConfig(*payload);
            if (HasError(error)) {
                ReportConfigUpdateError(
                    error.GetMessage(),
                    /*rollbackShallHelp=*/error.GetCode() == E_ARGUMENT);
                return;
            }

            newDynamicConfig = std::move(config);
        }

        if (google::protobuf::util::MessageDifferencer::Equals(
                newDynamicConfig,
                DynamicConfig))
        {
            LOG_INFO_S(
                ctx,
                TBlockStoreComponents::CONFIGS_MANAGER,
                "PrivateDatabaseConfig is unchanged; skipping publication");
            ReplyConfigNotificationResponse(ev, ctx);
            return;
        }

        const auto currentConfig = ConfigHolder->Get();
        newConfig = MakeRuntimeBlockstoreConfig(
            StaticConfig,
            StartupConfig,
            newDynamicConfig,
            StorageConfigControls,
            diagnostics,
            GetBlockstoreConfigExtraParameters(*currentConfig));
    } catch (...) {
        ReportConfigUpdateError(
            TStringBuilder()
                << "Failed to apply PrivateDatabaseConfig from CMS: "
                << CurrentExceptionMessage(),
            /*rollbackShallHelp=*/true);
        return;
    }

    StorageConfigControls->UpdateDefaults(
        newConfig->GetStorageConfig()->GetConfigProto());

    // Publish before notifying consumers and acknowledging the dispatcher
    // so the new configuration is already available to readers.
    ConfigHolder->Set(std::move(newConfig));
    DynamicConfig = std::move(newDynamicConfig);
    // TODO: to be used for mon page
    RuntimeDiagnostics = std::move(diagnostics);

    LOG_INFO_S(
        ctx,
        TBlockStoreComponents::CONFIGS_MANAGER,
        (DynamicConfig.ByteSizeLong()
             ? "Accepted PrivateDatabaseConfig"
             : "Reset PrivateDatabaseConfig"));

    // Do not wait for consumers: they read the provider at their safe point
    // and may skip intermediate publications.
    for (const auto& subscriber: Subscribers) {
        ctx.Send(
            subscriber,
            new TEvConfigsManager::TEvConfigChanged(ConfigHolder),
            IEventHandle::FlagTrackDelivery);
    }

    ReplyConfigNotificationResponse(ev, ctx);
}

// Send ConfigNotificationResponse to ConfigsDispatcher
void TConfigsManagerActor::ReplyConfigNotificationResponse(
    const TEvConsole::TEvConfigNotificationRequest::TPtr& ev,
    const TActorContext& ctx)
{
    ctx.Send(
        ev->Sender,
        new TEvConsole::TEvConfigNotificationResponse(ev->Get()->Record),
        0,
        ev->Cookie);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

NActors::IActor* CreateConfigsManager(TConfigsManagerArgs args)
{
    return new TConfigsManagerActor(std::move(args));
}

}   // namespace NCloud::NBlockStore
