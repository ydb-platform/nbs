#include "ss_proxy_actor.h"

#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/storage/core/volume_label.h>
#include <cloud/blockstore/libs/storage/ss_proxy/ss_proxy_events_private.h>

#include <cloud/storage/core/libs/common/helpers.h>
#include <cloud/storage/core/libs/common/media.h>

#include <contrib/ydb/core/tx/tx_proxy/proxy.h>
#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NSchemeShard;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration Timeout = TDuration::Seconds(20);

////////////////////////////////////////////////////////////////////////////////

class TDescribeVolumeActor final
    : public TActorBootstrapped<TDescribeVolumeActor>
{
private:
    const TRequestInfoPtr RequestInfo;

    const TStorageConfigPtr Config;
    const TString DiskId;
    const bool UseSchemeCache;

    bool FallbackRequest = false;

public:
    TDescribeVolumeActor(
        TRequestInfoPtr requestInfo,
        TStorageConfigPtr config,
        TString diskId,
        bool useSchemeCache);

    void DescribeVolumeFromCache(const TActorContext& ctx);

    void Bootstrap(const TActorContext& ctx);

private:
    void DescribeVolume(const TActorContext& ctx);

    void ReplyAndDie(
        const TActorContext& ctx,
        std::unique_ptr<TEvSSProxy::TEvDescribeVolumeResponse> response);

    void ReplyAndDie(
        const TActorContext& ctx,
        std::unique_ptr<TEvService::TEvDescribeVolumeResponse> response);

    bool CheckVolume(
        const TActorContext& ctx,
        const NKikimrBlockStore::TVolumeConfig& volumeConfig) const;

    TString GetFullPath() const;

    void HandleDescribeVolumeFromSchemeCacheResponse(
        const TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev,
        const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void HandleDescribeSchemeResponse(
        const TEvSSProxy::TEvDescribeSchemeResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleWakeup(
        const NActors::TEvents::TEvWakeup::TPtr& ev,
        const NActors::TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TDescribeVolumeActor::TDescribeVolumeActor(
        TRequestInfoPtr requestInfo,
        TStorageConfigPtr config,
        TString diskId,
        bool useSchemeCache)
    : RequestInfo(std::move(requestInfo))
    , Config(std::move(config))
    , DiskId(std::move(diskId))
    , UseSchemeCache(useSchemeCache)
{}

void TDescribeVolumeActor::Bootstrap(const TActorContext& ctx)
{
    ctx.Schedule(Timeout, new TEvents::TEvWakeup());
    if (UseSchemeCache){
        DescribeVolumeFromCache(ctx);
    } else {
        DescribeVolume(ctx);
    }
    Become(&TThis::StateWork);
}

bool TDescribeVolumeActor::CheckVolume(
    const TActorContext& ctx,
    const NKikimrBlockStore::TVolumeConfig& volumeConfig) const
{
    ui64 blocksCount = 0;
    for (const auto& partition: volumeConfig.GetPartitions()) {
        blocksCount += partition.GetBlockCount();
    }

    if (!blocksCount || !volumeConfig.GetBlockSize()) {
        LOG_ERROR(ctx, TBlockStoreComponents::SS_PROXY,
            "Broken config for volume %s",
            GetFullPath().Quote().data());
        return false;
    }

    return true;
}

TString TDescribeVolumeActor::GetFullPath() const
{
    TStringBuilder fullPath;
    fullPath << Config->GetSchemeShardDir() << '/';

    if (!FallbackRequest) {
        fullPath << DiskIdToPathDeprecated(DiskId);
    } else {
        fullPath << DiskIdToPath(DiskId);
    }

    return fullPath;
}

void TDescribeVolumeActor::DescribeVolume(const TActorContext& ctx)
{
    auto request =
        std::make_unique<TEvSSProxy::TEvDescribeSchemeRequest>(GetFullPath());

    NCloud::Send(ctx, MakeSSProxyServiceId(), std::move(request));
}

void TDescribeVolumeActor::DescribeVolumeFromCache(const TActorContext& ctx)
{
    LOG_ERROR(
        ctx,
        TBlockStoreComponents::SERVICE,
        "Volume %s: describe request from schemeCashe",
        DiskId.Quote().data());

    auto request = TAutoPtr<NSchemeCache::TSchemeCacheNavigate>(
        new NSchemeCache::TSchemeCacheNavigate());
    auto& entry = request->ResultSet.emplace_back();
    entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
    entry.SyncVersion = true;
    entry.RequestType =
        NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByPath;
    entry.Path = {GetFullPath()};
    auto event = std::make_unique<TEvTxProxySchemeCache::TEvNavigateKeySet>(
        std::move(request));

    NCloud::Send(ctx, MakeSchemeCacheID(), std::move(event));
}

void TDescribeVolumeActor::ReplyAndDie(
    const TActorContext& ctx,
    std::unique_ptr<TEvSSProxy::TEvDescribeVolumeResponse> response)
{
    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

void TDescribeVolumeActor::ReplyAndDie(
    const TActorContext& ctx,
    std::unique_ptr<TEvService::TEvDescribeVolumeResponse> response)
{
    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TDescribeVolumeActor::HandleDescribeSchemeResponse(
    const TEvSSProxy::TEvDescribeSchemeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto& error = msg->GetError();

    if (HasError(error)) {
        if (FACILITY_FROM_CODE(error.GetCode()) == FACILITY_SCHEMESHARD) {
            auto status =
                static_cast<NKikimrScheme::EStatus>(STATUS_FROM_CODE(error.GetCode()));
            // TODO: return E_NOT_FOUND instead of StatusPathDoesNotExist
            if (status == NKikimrScheme::StatusPathDoesNotExist) {
                if (!FallbackRequest) {
                    FallbackRequest = true;
                    DescribeVolume(ctx);
                    return;
                }
            }
        }

        ReplyAndDie(
            ctx,
            std::make_unique<TEvSSProxy::TEvDescribeVolumeResponse>(
                error,
                GetFullPath()));
        return;
    }

    const auto& pathDescription = msg->PathDescription;
    const auto pathType = pathDescription.GetSelf().GetPathType();

    if (pathType != NKikimrSchemeOp::EPathTypeBlockStoreVolume) {
        ReplyAndDie(
            ctx,
            std::make_unique<TEvSSProxy::TEvDescribeVolumeResponse>(
                MakeError(
                    E_INVALID_STATE,
                    TStringBuilder() << "Described path is not a blockstore volume: "
                        << GetFullPath().Quote()),
                GetFullPath()));
        return;
    }

    const auto& volumeDescription =
        pathDescription.GetBlockStoreVolumeDescription();

    const auto& descr = msg->PathDescription.GetBlockStoreVolumeDescription();
    // Emptiness of VolumeTabletId or any of partition tablet ids means that
    // blockstore volume is not configured by Hive yet.

    if (!descr.GetVolumeTabletId()) {
        ReplyAndDie(
            ctx,
            std::make_unique<TEvSSProxy::TEvDescribeVolumeResponse>(
                MakeError(
                    E_REJECTED,
                    TStringBuilder()
                        << "Blockstore volume " << GetFullPath().Quote()
                        << " has zero VolumeTabletId"),
                GetFullPath()));
        return;
    }

    for (const auto& part: descr.GetPartitions()) {
        if (!part.GetTabletId()) {
            auto error = MakeError(
                E_REJECTED,
                TStringBuilder()
                    << "Blockstore volume " << GetFullPath().Quote()
                    << " has zero TabletId for partition: "
                    << part.GetPartitionId());
            ReplyAndDie(
                ctx,
                std::make_unique<TEvSSProxy::TEvDescribeVolumeResponse>(
                    std::move(error),
                    GetFullPath()));
            return;
        }
    }

    const auto& volumeConfig = volumeDescription.GetVolumeConfig();

    if (!CheckVolume(ctx, volumeConfig)) {
        // re-try request until get correct config or timeout
        DescribeVolume(ctx);
        return;
    }

    auto response = std::make_unique<TEvSSProxy::TEvDescribeVolumeResponse>(
        msg->Path,
        pathDescription);

    ReplyAndDie(ctx, std::move(response));
}

void TDescribeVolumeActor::HandleDescribeVolumeFromSchemeCacheResponse(
    const TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev,
    const TActorContext& ctx)
{
    LOG_ERROR(
        ctx,
        TBlockStoreComponents::SERVICE,
        "Volume %s: describe response from schemeCashe catched",
        DiskId.Quote().data());

    const auto* msg = ev->Get();
    const auto& resultSet = msg->Request->ResultSet;
    Cerr<<"pre"<<Endl;
    if (resultSet.empty()) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::SERVICE,
            "Volume %s: scheme cache returned empty result set",
            DiskId.Quote().data());

        ReplyAndDie(
            ctx,
            std::make_unique<TEvSSProxy::TEvDescribeVolumeResponse>(
                MakeError(E_FAIL, "Empty result set from scheme cache")));
        return;
    }
    auto& item = msg->Request->ResultSet.front();
    Cerr<<"auto& item = msg->Request->ResultSet.front()"<<Endl;
    Cerr<<"status& "<< item.Status << Endl;

    if (item.Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
        Cerr<<"status2 "<< item.Status << Endl;
        TStringBuilder status;
        status << item.Status;

        LOG_ERROR(
            ctx,
            TBlockStoreComponents::SERVICE,
            TStringBuilder()
                << "Volume " << DiskId.Quote().data()
                << " : scheme cache resolution failed: status= " << status);

        ReplyAndDie(
            ctx,
            std::make_unique<TEvSSProxy::TEvDescribeVolumeResponse>(MakeError(
                E_FAIL,
                TStringBuilder() << "Resolution failed: " << item.Status)));
        Cerr<<"ReplyAndDie "<< item.Status << Endl;

        return;
    }
    Cerr<<"uto blockStoreVolumeDescription = item.BFACILITY_SCHEMESHARDlockStoreVolumeInfo->Description;"<<Endl;

    auto blockStoreVolumeDescription = item.BlockStoreVolumeInfo->Description;
    // Emptiness of VolumeTabletId or any of partition tablet ids means that
    // blockstore volume is not configured by Hive yet.

    if (!blockStoreVolumeDescription.GetVolumeTabletId()) {
        ReplyAndDie(
            ctx,
            std::make_unique<TEvSSProxy::TEvDescribeVolumeResponse>(
                MakeError(
                    E_REJECTED,
                    TStringBuilder()
                        << "Blockstore volume " << GetFullPath().Quote()
                        << " has zero VolumeTabletId"),
                GetFullPath()));
        return;
    }
    Cerr<<"if (!blockStoreVolumeDescription.GetVolumeTabletId()) {;"<<Endl;

    for (const auto& part: blockStoreVolumeDescription.GetPartitions()) {
        if (!part.GetTabletId()) {
            auto error = MakeError(
                E_REJECTED,
                TStringBuilder()
                    << "Blockstore volume " << GetFullPath().Quote()
                    << " has zero TabletId for partition: "
                    << part.GetPartitionId());
            ReplyAndDie(
                ctx,
                std::make_unique<TEvSSProxy::TEvDescribeVolumeResponse>(
                    std::move(error),
                    GetFullPath()));
            return;
        }
    }
    Cerr<<"    for (const auto& part: blockStoreVolumeDescription.GetPartitions()) {;"<<Endl;

    const auto& volumeConfig = blockStoreVolumeDescription.GetVolumeConfig();
    Cerr<<"CheckVolume"<<Endl;
    if (!CheckVolume(ctx, volumeConfig)) {
        // Trieng to use DescribeVolume request via schemeshard
        DescribeVolume(ctx);
        return;
    }

    NProto::TVolume volume;

    VolumeConfigToVolume(volumeConfig, volume);
    volume.SetTokenVersion(blockStoreVolumeDescription.GetTokenVersion());

    auto response = std::make_unique<TEvService::TEvDescribeVolumeResponse>();
    *response->Record.MutableVolume() = std::move(volume);

    ReplyAndDie(ctx, std::move(response));
}

void TDescribeVolumeActor::HandleWakeup(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    LOG_ERROR(ctx, TBlockStoreComponents::SS_PROXY,
        "Describe request timed out for volume %s",
        GetFullPath().Quote().data());

    auto response = std::make_unique<TEvSSProxy::TEvDescribeVolumeResponse>(
        MakeError(
            E_TIMEOUT,
            TStringBuilder() << "DescribeVolume timeout for volume: "
                << GetFullPath().Quote()
        ),
        GetFullPath()
    );

    ReplyAndDie(ctx, std::move(response));
}

STFUNC(TDescribeVolumeActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvSSProxy::TEvDescribeSchemeResponse, HandleDescribeSchemeResponse);
        HFunc(
            NKikimr::TEvTxProxySchemeCache::TEvNavigateKeySetResult,
            HandleDescribeVolumeFromSchemeCacheResponse);

        HFunc(TEvents::TEvWakeup, HandleWakeup);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::SS_PROXY,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TSSProxyActor::HandleDescribeVolume(
    const TEvSSProxy::TEvDescribeVolumeRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    NCloud::Register<TDescribeVolumeActor>(
        ctx,
        std::move(requestInfo),
        Config,
        msg->DiskId,
        Config->GetUseSchemeCacheForDescribeVolume());
}

}   // namespace NCloud::NBlockStore::NStorage
