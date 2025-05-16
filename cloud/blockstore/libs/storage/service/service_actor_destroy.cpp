#include "service_actor.h"

#include <cloud/blockstore/libs/storage/api/disk_registry.h>
#include <cloud/blockstore/libs/storage/api/disk_registry_proxy.h>
#include <cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/core/config.h>

#include <cloud/storage/core/libs/common/media.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TDestroyVolumeActor final
    : public TActorBootstrapped<TDestroyVolumeActor>
{
private:
    const TActorId Sender;
    const ui64 Cookie;

    const TDuration AttachedDiskDestructionTimeout;
    const TVector<TString> DestructionAllowedOnlyForDisksWithIdPrefixes;
    const TString DiskId;
    const bool DestroyIfBroken;
    const bool Sync;
    const ui64 FillGeneration;
    const TDuration Timeout;

    bool IsDiskRegistryBased = false;
    bool VolumeNotFoundInSS = false;

public:
    TDestroyVolumeActor(
        const TActorId& sender,
        ui64 cookie,
        TDuration attachedDiskDestructionTimeout,
        TVector<TString> destructionAllowedOnlyForDisksWithIdPrefixes,
        TString diskId,
        bool destroyIfBroken,
        bool sync,
        ui64 fillGeneration,
        TDuration timeout);

    void Bootstrap(const TActorContext& ctx);

private:
    void WaitReady(const TActorContext& ctx);
    void DestroyVolume(const TActorContext& ctx);
    void NotifyDiskRegistry(const TActorContext& ctx);
    void StatVolume(const TActorContext& ctx);
    void DeallocateDisk(const TActorContext& ctx);
    void GracefulShutdown(const TActorContext& ctx);
    NProto::TError CheckIfDestructionIsAllowed() const;

    void HandleModifyResponse(
        const TEvSSProxy::TEvModifyVolumeResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleWaitReadyResponse(
        const TEvVolume::TEvWaitReadyResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleStatVolumeResponse(
        const TEvService::TEvStatVolumeResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleMarkDiskForCleanupResponse(
        const TEvDiskRegistry::TEvMarkDiskForCleanupResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleDeallocateDiskResponse(
        const TEvDiskRegistry::TEvDeallocateDiskResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleGracefulShutdownResponse(
        const TEvVolume::TEvGracefulShutdownResponse::TPtr&
            ev,
        const TActorContext& ctx);

    void HandleTimeout(
        const TEvents::TEvWakeup::TPtr& ev,
        const TActorContext& ctx);

    void ReplyAndDie(const TActorContext& ctx, NProto::TError error);

private:
    STFUNC(StateWork);
};

////////////////////////////////////////////////////////////////////////////////

TDestroyVolumeActor::TDestroyVolumeActor(
        const TActorId& sender,
        ui64 cookie,
        TDuration attachedDiskDestructionTimeout,
        TVector<TString> destructionAllowedOnlyForDisksWithIdPrefixes,
        TString diskId,
        bool destroyIfBroken,
        bool sync,
        ui64 fillGeneration,
        TDuration timeout)
    : Sender(sender)
    , Cookie(cookie)
    , AttachedDiskDestructionTimeout(attachedDiskDestructionTimeout)
    , DestructionAllowedOnlyForDisksWithIdPrefixes(
          std::move(destructionAllowedOnlyForDisksWithIdPrefixes))
    , DiskId(std::move(diskId))
    , DestroyIfBroken(destroyIfBroken)
    , Sync(sync)
    , FillGeneration(fillGeneration)
    , Timeout(timeout)
{}

void TDestroyVolumeActor::Bootstrap(const TActorContext& ctx)
{
    ctx.Schedule(Timeout, new TEvents::TEvWakeup());
    if (DestroyIfBroken) {
        WaitReady(ctx);
    } else {
        StatVolume(ctx);
    }

    Become(&TThis::StateWork);
}

void TDestroyVolumeActor::WaitReady(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvVolume::TEvWaitReadyRequest>();
    request->Record.SetDiskId(DiskId);

    NCloud::Send(
        ctx,
        MakeVolumeProxyServiceId(),
        std::move(request)
    );
}

void TDestroyVolumeActor::DestroyVolume(const TActorContext& ctx)
{
    NCloud::Send(
        ctx,
        MakeSSProxyServiceId(),
        std::make_unique<TEvSSProxy::TEvModifyVolumeRequest>(
            TEvSSProxy::TModifyVolumeRequest::EOpType::Destroy,
            DiskId,
            "", // newMountToken
            0,  // tokenVersion
            FillGeneration));
}

void TDestroyVolumeActor::NotifyDiskRegistry(const TActorContext& ctx)
{
    LOG_DEBUG(ctx, TBlockStoreComponents::SERVICE, "notify disk registry");

    auto request = std::make_unique<TEvDiskRegistry::TEvMarkDiskForCleanupRequest>();
    request->Record.SetDiskId(DiskId);

    NCloud::Send(ctx, MakeDiskRegistryProxyServiceId(), std::move(request));
}

void TDestroyVolumeActor::StatVolume(const TActorContext& ctx)
{
    LOG_DEBUG(ctx, TBlockStoreComponents::SERVICE, "stat volume");

    auto request = std::make_unique<TEvService::TEvStatVolumeRequest>();
    request->Record.SetDiskId(DiskId);
    // no need to check partition readiness and retrieve partition stats
    request->Record.SetNoPartition(true);

    NCloud::Send(
        ctx,
        MakeVolumeProxyServiceId(),
        std::move(request));
}

void TDestroyVolumeActor::DeallocateDisk(const TActorContext& ctx)
{
    LOG_INFO(ctx, TBlockStoreComponents::SERVICE,
        "deallocate disk (Sync=%d)", Sync);

    auto request = std::make_unique<TEvDiskRegistry::TEvDeallocateDiskRequest>();
    request->Record.SetDiskId(DiskId);
    request->Record.SetSync(Sync);

    NCloud::Send(ctx, MakeDiskRegistryProxyServiceId(), std::move(request));
}

void TDestroyVolumeActor::GracefulShutdown(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvVolume::TEvGracefulShutdownRequest>();
    request->Record.SetDiskId(DiskId);
    NCloud::Send(ctx, MakeVolumeProxyServiceId(), std::move(request));
}

NProto::TError TDestroyVolumeActor::CheckIfDestructionIsAllowed() const
{
    const auto& prefixes = DestructionAllowedOnlyForDisksWithIdPrefixes;
    if (prefixes) {
        const bool allowed = AnyOf(
            prefixes,
            [&] (const auto& prefix) {
                return DiskId.StartsWith(prefix);
            }
        );
        if (!allowed) {
            return MakeError(
                E_ARGUMENT,
                TStringBuilder() << "DiskId: " << DiskId
                << ", only disks with specific id prefixes are allowed to be"
                << " deleted");
        }
    }

    return MakeError(S_OK);
}

void TDestroyVolumeActor::HandleModifyResponse(
    const TEvSSProxy::TEvModifyVolumeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto& error = msg->GetError();

    if (HasError(error)) {
        LOG_ERROR(ctx, TBlockStoreComponents::SERVICE,
            "Volume %s: drop failed, error %s",
            DiskId.Quote().data(),
            FormatError(error).data());

        ReplyAndDie(ctx, error);
        return;
    }

    LOG_INFO(ctx, TBlockStoreComponents::SERVICE,
        "Volume %s dropped successfully",
        DiskId.Quote().c_str());

    if (IsDiskRegistryBased) {
        DeallocateDisk(ctx);

        return;
    }

    ReplyAndDie(ctx, error);
}

void TDestroyVolumeActor::HandleWaitReadyResponse(
    const TEvVolume::TEvWaitReadyResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto& error = msg->GetError();

    if (HasError(error)) {
        LOG_DEBUG(ctx, TBlockStoreComponents::SERVICE,
            "Volume %s WaitReady error %s",
            DiskId.Quote().c_str(),
            error.GetMessage().Quote().c_str());

        DestroyVolume(ctx);
    } else {
        LOG_DEBUG(ctx, TBlockStoreComponents::SERVICE,
            "Volume %s WaitReady success",
            DiskId.Quote().c_str());

        ReplyAndDie(
            ctx,
            MakeError(
                E_INVALID_STATE,
                "this volume is online, it can't be destroyed"));
    }
}

void TDestroyVolumeActor::HandleMarkDiskForCleanupResponse(
    const TEvDiskRegistry::TEvMarkDiskForCleanupResponse::TPtr& ev,
    const TActorContext& ctx)
{
    LOG_DEBUG(ctx, TBlockStoreComponents::SERVICE, "handle response from disk registry");

    const auto* msg = ev->Get();
    const auto& error = msg->GetError();

    // disk is broken and will be removed by DR at some point
    if (error.GetCode() == E_NOT_FOUND) {
        LOG_INFO(
            ctx,
            TBlockStoreComponents::SERVICE,
            "volume %s not found in registry",
            DiskId.Quote().data());
        DestroyVolume(ctx);
        return;
    }

    if (HasError(error)) {
        LOG_ERROR(ctx, TBlockStoreComponents::SERVICE,
            "Volume %s: unable to notify DR about disk destruction: %s",
            DiskId.Quote().data(),
            FormatError(error).data());

        ReplyAndDie(ctx, error);
        return;
    }

    GracefulShutdown(ctx);
}

void TDestroyVolumeActor::HandleDeallocateDiskResponse(
    const TEvDiskRegistry::TEvDeallocateDiskResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto& error = msg->GetError();

    if (HasError(error)) {
        LOG_ERROR(ctx, TBlockStoreComponents::SERVICE,
            "Volume %s: unable to deallocate disk: %s",
            DiskId.Quote().data(),
            FormatError(error).data());
    } else {
        LOG_INFO(ctx, TBlockStoreComponents::SERVICE,
            "Volume %s deallocated successfully",
            DiskId.Quote().c_str());
    }

    ReplyAndDie(ctx, VolumeNotFoundInSS && error.GetCode() == S_ALREADY
        ? MakeError(S_ALREADY, "volume not found")
        : error);
}

void TDestroyVolumeActor::HandleStatVolumeResponse(
    const TEvService::TEvStatVolumeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    LOG_DEBUG(ctx, TBlockStoreComponents::SERVICE, "handle stat response");

    const auto* msg = ev->Get();

    if (msg->GetStatus() ==
        MAKE_SCHEMESHARD_ERROR(NKikimrScheme::StatusPathDoesNotExist))
    {
        if (Sync) {
            VolumeNotFoundInSS = true;
            DeallocateDisk(ctx);
        } else {
            ReplyAndDie(ctx, MakeError(S_ALREADY, "volume not found"));
        }
        return;
    }

    if (auto error = msg->GetError(); HasError(error)) {
        LOG_ERROR(ctx, TBlockStoreComponents::SERVICE,
            "Volume %s: unable to stat volume: %s",
            DiskId.Quote().data(),
            FormatError(error).data());

        ReplyAndDie(ctx, error);
        return;
    }

    if (auto error = CheckIfDestructionIsAllowed(); HasError(error)) {
        ReplyAndDie(ctx, std::move(error));
        return;
    }

    for (const auto& client: msg->Record.GetClients()) {
        if (client.GetInstanceId()) {
            const auto timeout = AttachedDiskDestructionTimeout;
            const auto disconnectTimestamp =
                TInstant::MicroSeconds(client.GetDisconnectTimestamp());
            const bool isStale = disconnectTimestamp
                && disconnectTimestamp + timeout < ctx.Now();
            if (isStale) {
                LOG_WARN(ctx, TBlockStoreComponents::SERVICE,
                    "Volume %s is attached to instance (stale): %s, dt: %s",
                    DiskId.Quote().c_str(),
                    client.GetInstanceId().Quote().c_str(),
                    ToString(disconnectTimestamp).c_str());
            } else {
                LOG_WARN(ctx, TBlockStoreComponents::SERVICE,
                    "Volume %s is attached to instance (active): %s",
                    DiskId.Quote().c_str(),
                    client.GetInstanceId().Quote().c_str());

                ReplyAndDie(
                    ctx,
                    MakeError(
                        E_REJECTED,
                        TStringBuilder() << "attached to an active instance: "
                                         << client.GetInstanceId()));
                return;
            }
        }
    }

    IsDiskRegistryBased = IsDiskRegistryMediaKind(
        msg->Record.GetVolume().GetStorageMediaKind());

    if (IsDiskRegistryBased) {
        NotifyDiskRegistry(ctx);
    } else {
        DestroyVolume(ctx);
    }
}

void TDestroyVolumeActor::HandleGracefulShutdownResponse(
    const TEvVolume::TEvGracefulShutdownResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (auto error = msg->GetError(); HasError(error)) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::SERVICE,
            "Volume %s: unable to gracefully stop volume: %s",
            DiskId.Quote().data(),
            FormatError(error).data());

        ReplyAndDie(ctx, std::move(error));
        return;
    }

    DestroyVolume(ctx);
}

void TDestroyVolumeActor::HandleTimeout(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    LOG_ERROR(
        ctx,
        TBlockStoreComponents::SERVICE,
        "Timeout destroy volume request, diskId = %s, destroyIfBroken = %d, "
        "sync = %d",
        DiskId.c_str(),
        DestroyIfBroken,
        Sync);

    ReplyAndDie(ctx, MakeError(E_TIMEOUT, "Timeout"));
}

void TDestroyVolumeActor::ReplyAndDie(
    const TActorContext& ctx,
    NProto::TError error)
{
    auto response = std::make_unique<TEvService::TEvDestroyVolumeResponse>(
        std::move(error));

    NCloud::Send(ctx, Sender, std::move(response), Cookie);
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TDestroyVolumeActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvSSProxy::TEvModifyVolumeResponse, HandleModifyResponse);
        HFunc(TEvVolume::TEvWaitReadyResponse, HandleWaitReadyResponse);
        HFunc(
            TEvDiskRegistry::TEvMarkDiskForCleanupResponse,
            HandleMarkDiskForCleanupResponse);
        HFunc(
            TEvDiskRegistry::TEvDeallocateDiskResponse,
            HandleDeallocateDiskResponse);

        HFunc(
            TEvService::TEvStatVolumeResponse,
            HandleStatVolumeResponse);

        HFunc(
            TEvVolume::TEvGracefulShutdownResponse,
            HandleGracefulShutdownResponse);

        HFunc(TEvents::TEvWakeup, HandleTimeout);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::SERVICE,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TServiceActor::HandleDestroyVolume(
    const TEvService::TEvDestroyVolumeRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto& request = msg->Record;
    const auto& diskId = request.GetDiskId();
    const bool destroyIfBroken = request.GetDestroyIfBroken();
    const bool sync = request.GetSync();
    const ui64 fillGeneration = request.GetFillGeneration();

    LOG_INFO(ctx, TBlockStoreComponents::SERVICE,
        "Deleting volume: diskId = %s, destroyIfBroken = %d, sync = %d, fillGeneration = %" PRIu64,
        diskId.Quote().c_str(),
        destroyIfBroken,
        sync,
        fillGeneration);

    NCloud::Register<TDestroyVolumeActor>(
        ctx,
        ev->Sender,
        ev->Cookie,
        Config->GetAttachedDiskDestructionTimeout(),
        Config->GetDestructionAllowedOnlyForDisksWithIdPrefixes(),
        diskId,
        destroyIfBroken,
        sync,
        fillGeneration,
        Config->GetDestroyVolumeTimeout());
}

}   // namespace NCloud::NBlockStore::NStorage
