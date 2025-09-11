#include "ss_proxy_actor.h"

#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/volume_label.h>
#include <cloud/blockstore/libs/storage/ss_proxy/ss_proxy_events_private.h>

#include <cloud/storage/core/libs/common/helpers.h>

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
    enum class EState
    {
        DescribePrimaryDeprecated,
        DescribePrimary,
        DescribeSecondary,
    };

    const TRequestInfoPtr RequestInfo;
    const TStorageConfigPtr Config;
    const TString DiskId;

    EState State = EState::DescribePrimaryDeprecated;

public:
    TDescribeVolumeActor(
        TRequestInfoPtr requestInfo,
        TStorageConfigPtr config,
        TString diskId);

    void Bootstrap(const TActorContext& ctx);

private:
    void DescribeVolume(const TActorContext& ctx);

    void ReplyAndDie(
        const TActorContext& ctx,
        std::unique_ptr<TEvSSProxy::TEvDescribeVolumeResponse> response);

    bool CheckVolume(
        const TActorContext& ctx,
        const NKikimrBlockStore::TVolumeConfig& volumeConfig) const;

    TString GetFullPath() const;

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
        TString diskId)
    : RequestInfo(std::move(requestInfo))
    , Config(std::move(config))
    , DiskId(std::move(diskId))
{}

void TDescribeVolumeActor::Bootstrap(const TActorContext& ctx)
{
    ctx.Schedule(Timeout, new TEvents::TEvWakeup());
    DescribeVolume(ctx);
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

    switch (State) {
        case EState::DescribePrimaryDeprecated:
            fullPath << DiskIdToPathDeprecated(DiskId);
            break;
        case EState::DescribePrimary:
            fullPath << DiskIdToPath(DiskId);
            break;
        case EState::DescribeSecondary:
            fullPath << DiskIdToPath(GetSecondaryDiskId(DiskId));
            break;
    }

    return fullPath;
}

void TDescribeVolumeActor::DescribeVolume(const TActorContext& ctx)
{
    auto request =
        std::make_unique<TEvSSProxy::TEvDescribeSchemeRequest>(GetFullPath());

    NCloud::Send(ctx, MakeSSProxyServiceId(), std::move(request));
}

void TDescribeVolumeActor::ReplyAndDie(
    const TActorContext& ctx,
    std::unique_ptr<TEvSSProxy::TEvDescribeVolumeResponse> response)
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

            LOG_TRACE(
                ctx,
                TBlockStoreComponents::SS_PROXY,
                "Describe request error for volume %s %s %s",
                DiskId.c_str(),
                GetFullPath().Quote().data(),
                FormatError(error).c_str());

            if (status == NKikimrScheme::StatusPathDoesNotExist) {
                switch (State) {
                    case EState::DescribePrimaryDeprecated: {
                        State = EState::DescribePrimary;
                        DescribeVolume(ctx);
                        return;
                    }
                    case EState::DescribePrimary: {
                        State = EState::DescribeSecondary;
                        DescribeVolume(ctx);
                        return;
                    }
                    case EState::DescribeSecondary:
                        break;
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
        msg->DiskId);
}

}   // namespace NCloud::NBlockStore::NStorage
