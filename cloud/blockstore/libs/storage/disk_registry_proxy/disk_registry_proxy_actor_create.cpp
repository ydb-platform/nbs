#include "disk_registry_proxy_actor.h"

#include <cloud/blockstore/libs/kikimr/helpers.h>
#include <cloud/blockstore/libs/storage/api/disk_registry.h>
#include <cloud/blockstore/libs/storage/api/disk_registry_proxy.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/monitoring_utils.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/storage/core/tenant.h>
#include <cloud/blockstore/libs/storage/disk_registry_proxy/model/config.h>

#include <cloud/storage/core/libs/api/hive_proxy.h>

#include <ydb/core/base/appdata.h>
#include <ydb/core/mon/mon.h>
#include <ydb/core/tablet/tablet_pipe_client_cache.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/log.h>
#include <library/cpp/monlib/service/pages/templates.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

using namespace NCloud::NStorage;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 DrChannelCount = 3;

////////////////////////////////////////////////////////////////////////////////

std::array<TString, DrChannelCount> GetPoolKinds(
    const TDiskRegistryChannelKinds& kinds,
    const TStorageConfig& config)
{
    return {
        kinds.SysKind ? kinds.SysKind : config.GetSSDSystemChannelPoolKind(),
        kinds.LogKind ? kinds.LogKind : config.GetSSDLogChannelPoolKind(),
        kinds.IndexKind ? kinds.IndexKind : config.GetSSDIndexChannelPoolKind(),
    };
}

////////////////////////////////////////////////////////////////////////////////

class TCreateDiskRegistryActor final
    : public TActorBootstrapped<TCreateDiskRegistryActor>
{
private:
    const TStorageConfigPtr StorageConfig;
    const TDiskRegistryProxyConfigPtr Config;
    const TActorId Sender;
    const TDiskRegistryChannelKinds Kinds;

public:
    TCreateDiskRegistryActor(
        TStorageConfigPtr config,
        TDiskRegistryProxyConfigPtr diskRegistryProxyConfig,
        TActorId requester,
        TDiskRegistryChannelKinds kinds);

    void Bootstrap(const TActorContext& ctx);

private:
    void ReplyAndDie(const TActorContext& ctx, ui64 tabletId);
    void ReplyAndDie(const TActorContext& ctx, NProto::TError error);

private:
    STFUNC(StateWork);

    void HandleCreateTablet(
        TEvHiveProxy::TEvCreateTabletResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleDescribeSchemeResponse(
        TEvSSProxy::TEvDescribeSchemeResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TCreateDiskRegistryActor::TCreateDiskRegistryActor(
        TStorageConfigPtr config,
        TDiskRegistryProxyConfigPtr diskRegistryProxyConfig,
        TActorId sender,
        TDiskRegistryChannelKinds kinds)
    : StorageConfig(std::move(config))
    , Config(std::move(diskRegistryProxyConfig))
    , Sender(std::move(sender))
    , Kinds(std::move(kinds))
{}

void TCreateDiskRegistryActor::Bootstrap(const TActorContext& ctx)
{
    TThis::Become(&TThis::StateWork);

    auto request = std::make_unique<TEvSSProxy::TEvDescribeSchemeRequest>(
        StorageConfig->GetSchemeShardDir());

    NCloud::Send(
        ctx,
        MakeSSProxyServiceId(),
        std::move(request),
        0);
}

void TCreateDiskRegistryActor::ReplyAndDie(
    const TActorContext& ctx,
    ui64 tabletId)
{
    auto response = std::make_unique<
        TEvDiskRegistryProxyPrivate::TEvDiskRegistryCreateResult>(
        tabletId,
        Kinds);
    NCloud::Send(ctx, Sender, std::move(response));

    Die(ctx);
}

void TCreateDiskRegistryActor::ReplyAndDie(
    const TActorContext& ctx,
    NProto::TError error)
{
    auto response = std::make_unique<
        TEvDiskRegistryProxyPrivate::TEvDiskRegistryCreateResult>(
        error,
        0,
        Kinds);
    NCloud::Send(ctx, Sender, std::move(response));

    Die(ctx);
}

void TCreateDiskRegistryActor::HandleDescribeSchemeResponse(
    TEvSSProxy::TEvDescribeSchemeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    if (ev->Cookie) {
        LOG_DEBUG(ctx, TBlockStoreComponents::DISK_REGISTRY_PROXY,
            "Received unexpected DescribeScheme response");

        return;
    }

    LOG_DEBUG(ctx, TBlockStoreComponents::DISK_REGISTRY_PROXY,
        "Received DescribeScheme response");

    auto* msg = ev->Get();

    if (HasError(msg->Error)) {
        LOG_ERROR_S(ctx, TBlockStoreComponents::DISK_REGISTRY_PROXY,
            "Can't create Disk Registry tablet. Error on describe scheme: "
            << FormatError(msg->Error));

        ReplyAndDie(ctx, std::move(msg->Error));
        return;
    }

    const auto& descr = msg->PathDescription.GetDomainDescription();

    if (descr.StoragePoolsSize() == 0) {
        LOG_ERROR_S(ctx, TBlockStoreComponents::DISK_REGISTRY_PROXY,
            "Can't create Disk Registry tablet. Empty storage pool");

        ReplyAndDie(
            ctx,
            MakeError(E_FAIL, "Can't create Disk Registry tablet. Empty storage pool"));
        return;
    }

    NKikimrHive::TEvCreateTablet request;

    request.SetOwner(Config->GetOwner());
    request.SetOwnerIdx(Config->GetOwnerIdx());
    request.SetTabletType(TTabletTypes::BlockStoreDiskRegistry);

    *request.AddAllowedDomains() = descr.GetDomainKey();

    for (const auto& channelKind: GetPoolKinds(Kinds, *StorageConfig)) {
        auto* pool = FindIfPtr(descr.GetStoragePools(), [&] (const auto& pool) {
            return pool.GetKind() == channelKind;
        });
        if (!pool) {
            TStringBuilder pools;
            for (const auto& pool: descr.GetStoragePools()) {
                pools << '[' << pool.GetName() << "," << pool.GetKind() << ']';
            }

            auto b = TStringBuilder() <<
                "Can't create Disk Registry tablet. No pool for " <<
                channelKind.Quote() <<
                " but only " <<
                pools <<
                " available";

            LOG_ERROR_S(ctx, TBlockStoreComponents::DISK_REGISTRY_PROXY, b);

            ReplyAndDie(ctx, MakeError(E_FAIL, b));
            return;
        }
        request.AddBindedChannels()->SetStoragePoolName(pool->GetName());
    }

    const auto hiveTabletId = GetHiveTabletId(StorageConfig, ctx);

    LOG_INFO_S(ctx, TBlockStoreComponents::DISK_REGISTRY_PROXY,
        "Create Disk Registry Tablet. Hive: " << hiveTabletId);

    NCloud::Send<TEvHiveProxy::TEvCreateTabletRequest>(
        ctx,
        MakeHiveProxyServiceId(),
        ev->Cookie,
        hiveTabletId,
        std::move(request));
}

void TCreateDiskRegistryActor::HandleCreateTablet(
    TEvHiveProxy::TEvCreateTabletResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (ev->Cookie) {
        LOG_DEBUG(ctx, TBlockStoreComponents::DISK_REGISTRY_PROXY,
            "Received expired CreateTablet response");

        return;
    }

    if (!HasError(msg->Error)) {
        ReplyAndDie(ctx, msg->TabletId);
        return;
    }

    LOG_ERROR_S(ctx, TBlockStoreComponents::DISK_REGISTRY_PROXY,
        "Can't create Disk Registry tablet: " << FormatError(msg->Error));
    ReplyAndDie(ctx, msg->Error);
}

STFUNC(TCreateDiskRegistryActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvSSProxy::TEvDescribeSchemeResponse, HandleDescribeSchemeResponse);
        HFunc(TEvHiveProxy::TEvCreateTabletResponse, HandleCreateTablet);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::DISK_REGISTRY_PROXY,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryProxyActor::CreateTablet(
    const NActors::TActorContext& ctx,
    TDiskRegistryChannelKinds kinds)
{
    NCloud::Register<TCreateDiskRegistryActor>(
        ctx,
        StorageConfig,
        Config,
        SelfId(),
        std::move(kinds));
}

void TDiskRegistryProxyActor::HandleCreateResult(
    const TEvDiskRegistryProxyPrivate::TEvDiskRegistryCreateResult::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (HasError(msg->GetError()) &&
        GetErrorKind(msg->GetError()) == EErrorKind::ErrorRetriable)
    {
        LOG_WARN_S(
            ctx,
            TBlockStoreComponents::DISK_REGISTRY_PROXY,
            "CreateTablet failed with a retriable error: "
                << FormatError(msg->GetError()));

        ++RequestId;
        ctx.Schedule(
            Config->GetRetryLookupTimeout(),
            new TEvDiskRegistryProxyPrivate::TEvCreateTabletRequest(
                std::move(msg->Kinds)));
        return;
    }

    if (!ReassignRequestInfo) {
        if (!FAILED(msg->GetStatus())) {
            StartWork(msg->TabletId, ctx);
        } else {
            TThis::Become(&TThis::StateError);
        }
        return;
    }

    auto response = std::make_unique<TEvDiskRegistryProxy::TEvReassignResponse>(
        std::move(msg->Error)
    );

    NCloud::Reply(ctx, *ReassignRequestInfo, std::move(response));
    ReassignRequestInfo.Reset();
}

}   // namespace NCloud::NBlockStore::NStorage
