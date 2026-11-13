#include "configs_manager.h"

#include "configs_manager_renderer.h"
#include "events.h"

#include <cloud/blockstore/libs/config/blockstore_config_management.h>
#include <cloud/blockstore/libs/config/opaque_config_parser.h>
#include <cloud/blockstore/libs/config/sections_update.h>
#include <cloud/blockstore/libs/kikimr/components.h>

#include <cloud/storage/core/libs/actors/helpers.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/proto_helpers.h>
#include <cloud/storage/core/libs/config/runtime_config.h>
#include <cloud/storage/core/libs/diagnostics/critical_events.h>

#include <contrib/ydb/core/base/appdata.h>
#include <contrib/ydb/core/cms/console/configs_dispatcher.h>
#include <contrib/ydb/core/cms/console/console.h>
#include <contrib/ydb/core/mon/mon.h>
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
void ReportConfigUpdateError(
    TBlockstoreConfigRendererData& rendererData,
    const TString& reason,
    bool rollbackShallHelp)
{
    rendererData.RejectedUpdateReason = reason;
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

// Report each section's issues and collect every rejection for monitoring.
TString ReportConfigSectionsIssues(
    const TConfigSectionsUpdateIssues& updateIssues,
    const TActorContext& ctx)
{
    TString rejectedSectionsReason;
    for (const auto& section: updateIssues.Sections) {
        // Keep nonfatal skips visible even if another rule rejects the section.
        for (const auto& issue: section.Issues) {
            LOG_WARN_S(ctx, TBlockStoreComponents::CONFIGS_MANAGER, issue);
        }
        if (!HasError(section.RejectionError)) {
            continue;
        }

        // Report each rejection and preserve their order in the monitoring
        // text.
        const auto reason = FormatConfigSectionRejection(section);
        if (!rejectedSectionsReason.empty()) {
            rejectedSectionsReason += "; ";
        }
        rejectedSectionsReason += reason;
        ReportDynamicConfigError(
            TStringBuilder()
            << reason << ". Keeping the last accepted dynamic " << section.Path
            << " section. Fix or roll back the configuration before restarting"
               " NBS: the retained section is kept only in memory.");
    }
    return rejectedSectionsReason;
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

    // Last accepted PrivateDatabaseConfig before normalization or merging;
    // empty without a dynamic source.
    NProto::TBlockstoreConfig DynamicConfig;

    // Monitoring metadata owned by the renderer and changed only by this actor.
    TBlockstoreConfigRenderer ConfigRenderer;

    // ICB overrides shared by every published StorageConfig; non-null.
    const NStorage::TStorageConfigControlsPtr StorageConfigControls;

    // ConfigsDispatcher actor; zero selects the node-local service.
    const TActorId ConfigsDispatcherId;

    // Local recipients of publication notices; consumers remove their own
    // subscriptions before stopping.
    THashSet<TActorId> Subscribers;

    STFUNC(StateWork);

    // Register the read-only configuration page when monitoring is available.
    void RegisterPages(const TActorContext& ctx);

    // Reply with the sources and the current publication, including live ICB.
    void Handle(const NMon::TEvHttpInfo::TPtr& ev, const TActorContext& ctx);

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
{
    ConfigRenderer.Data.StartupDynamicConfig = DynamicConfig;
    ConfigRenderer.Data.DynamicConfigPresent =
        HasKnownFields(DynamicConfig);
    ConfigRenderer.Data.RejectedUpdateReason =
        std::move(args.StartupRejectedSectionsReason);
    if (ConfigRenderer.Data.RejectedUpdateReason) {
        ConfigRenderer.Data.UpdateStatus =
            EConfigUpdateStatus::StartupPartiallyApplied;
    }
}

void TConfigsManagerActor::Bootstrap(const TActorContext& ctx)
{
    ConfigRenderer.Data.LastUpdateTime = ctx.Now();
    RegisterPages(ctx);

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
        HFunc(NMon::TEvHttpInfo, Handle);
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

void TConfigsManagerActor::RegisterPages(const TActorContext& ctx)
{
    auto* mon = NKikimr::AppData(ctx)->Mon;
    if (!mon) {
        return;
    }

    auto* rootPage = mon->RegisterIndexPage("blockstore", "BlockStore");
    mon->RegisterActorPage(
        rootPage,
        "configs_manager",
        "ConfigsManager",
        false,
        ctx.ActorSystem(),
        SelfId());
}

void TConfigsManagerActor::Handle(
    const NMon::TEvHttpInfo::TPtr& ev,
    const TActorContext& ctx)
{
    auto html = ConfigRenderer.RenderHtml(
        *ConfigHolder->Get(),
        StaticConfig,
        DynamicConfig,
        *StorageConfigControls);

    ctx.Send(
        ev->Sender,
        new NMon::TEvHttpInfoRes(std::move(html)),
        0,
        ev->Cookie);
}

void TConfigsManagerActor::Handle(
    const TEvConfigsManager::TEvSetConfigSubscriptionRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto subscriber =
        ev->Get()->Subscriber ? ev->Get()->Subscriber : ev->Sender;
    Subscribers.insert(subscriber);

    const auto config = ConfigHolder->Get();

    // Confirm registration to the requester, then initialize the recipient
    // even when another actor requested the subscription on its behalf.
    ctx.Send(
        ev->Sender,
        new TEvConfigsManager::TEvSetConfigSubscriptionResponse(config),
        0,
        ev->Cookie);
    ctx.Send(
        subscriber,
        new TEvConfigsManager::TEvConfigChanged(config),
        IEventHandle::FlagTrackDelivery);
}

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

void TConfigsManagerActor::Handle(const TEvents::TEvUndelivered::TPtr& ev)
{
    const auto* message = ev->Get();
    if (message->SourceType == TEvConfigsManager::TEvConfigChanged::EventType &&
        message->Reason == TEvents::TEvUndelivered::ReasonActorUnknown)
    {
        Subscribers.erase(ev->Sender);
    }
}

void TConfigsManagerActor::Handle(
    const TEvConsole::TEvConfigNotificationRequest::TPtr& ev,
    const TActorContext& ctx)
{
    LOG_INFO_S(
        ctx,
        TBlockStoreComponents::CONFIGS_MANAGER,
        "Received YAML configuration from ConfigsDispatcher");

    const auto* message = ev->Get();
    if (!ConfigRenderer.Data.StartupDeliveryCookie) {
        ConfigRenderer.Data.StartupDeliveryCookie = ev->Cookie;
    }
    NProto::TBlockstoreConfig newDynamicConfig;
    NConfig::TRuntimeConfigDiagnostics diagnostics;
    IBlockstoreConfigPtr newConfig;
    TConfigSectionsUpdateIssues updateIssues;
    TString rejectedSectionsReason;

    // Prepare the complete update before changing the publication or live
    // controls, so an exception leaves the last accepted state intact.
    try {
        if (auto it = message->OpaqueConfigs.find(PrivateDatabaseConfigKind);
            it != message->OpaqueConfigs.end())
        {
            const auto payload = it->second;
            if (!payload) {
                ReportConfigUpdateError(
                    ConfigRenderer.Data,
                    "Internal error: received a null PrivateDatabaseConfig "
                    "payload from ConfigsDispatcher",
                    /*rollbackShallHelp=*/false);
                return;
            }

            auto [config, error] = ExtractBlockstoreConfig(*payload);
            if (HasError(error)) {
                ReportConfigUpdateError(
                    ConfigRenderer.Data,
                    error.GetMessage(),
                    /*rollbackShallHelp=*/error.GetCode() == E_ARGUMENT);
                return;
            }

            updateIssues =
                PrepareDynamicConfigSectionsUpdate(config, &DynamicConfig);
            rejectedSectionsReason =
                ReportConfigSectionsIssues(updateIssues, ctx);

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
            ConfigRenderer.Data.DynamicConfigPresent =
                message->OpaqueConfigs.contains(PrivateDatabaseConfigKind);
            // Preserve section rejection diagnostics even when the dynamic
            // configuration is unchanged.
            // If no sections are rejected, treat an identical first delivery as
            // startup even if the CMS version is newer: the label describes
            // the configuration values applied at startup.
            const bool isStartupDelivery =
                (ConfigRenderer.Data.UpdateStatus ==
                     EConfigUpdateStatus::Startup ||
                 ConfigRenderer.Data.UpdateStatus ==
                     EConfigUpdateStatus::StartupPartiallyApplied) &&
                ev->Cookie == *ConfigRenderer.Data.StartupDeliveryCookie;
            if (isStartupDelivery) {
                ConfigRenderer.Data.UpdateStatus =
                    updateIssues.HasRejectedSections()
                        ? EConfigUpdateStatus::StartupPartiallyApplied
                        : EConfigUpdateStatus::Startup;
            } else {
                ConfigRenderer.Data.LastUpdateTime = ctx.Now();
                ConfigRenderer.Data.UpdateStatus =
                    updateIssues.HasRejectedSections()
                        ? EConfigUpdateStatus::RuntimePartiallyApplied
                        : EConfigUpdateStatus::RuntimeUnchanged;
            }
            ConfigRenderer.Data.RejectedUpdateReason =
                std::move(rejectedSectionsReason);
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
            ConfigRenderer.Data,
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
    const IBlockstoreConfigConstPtr publishedConfig = newConfig;
    ConfigHolder->Set(std::move(newConfig));
    DynamicConfig = std::move(newDynamicConfig);
    ConfigRenderer.Data.DynamicConfigPresent =
        message->OpaqueConfigs.contains(PrivateDatabaseConfigKind);
    ConfigRenderer.Data.LastUpdateTime = ctx.Now();
    ConfigRenderer.Data.UpdateStatus =
        updateIssues.HasRejectedSections()
            ? EConfigUpdateStatus::RuntimePartiallyApplied
            : EConfigUpdateStatus::Runtime;
    ConfigRenderer.Data.RejectedUpdateReason =
        std::move(rejectedSectionsReason);
    ConfigRenderer.Data.RuntimeDiagnostics = std::move(diagnostics);

    LOG_INFO_S(
        ctx,
        TBlockStoreComponents::CONFIGS_MANAGER,
        (ConfigRenderer.Data.UpdateStatus ==
                 EConfigUpdateStatus::RuntimePartiallyApplied
             ? "Partially accepted PrivateDatabaseConfig"
             : (HasKnownFields(DynamicConfig)
                    ? "Accepted PrivateDatabaseConfig"
                    : "Reset PrivateDatabaseConfig")));

    // Do not wait for consumers: they process the snapshot carried by each
    // notification at their safe point.
    for (const auto& subscriber: Subscribers) {
        ctx.Send(
            subscriber,
            new TEvConfigsManager::TEvConfigChanged(publishedConfig),
            IEventHandle::FlagTrackDelivery);
    }

    ReplyConfigNotificationResponse(ev, ctx);
}

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
