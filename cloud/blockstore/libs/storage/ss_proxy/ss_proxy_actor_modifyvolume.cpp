#include "ss_proxy_actor.h"

#include <cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/volume_label.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using EOpType = TEvSSProxy::TModifyVolumeRequest::EOpType;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TModifyVolumeActor final
    : public TActorBootstrapped<TModifyVolumeActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TStorageConfigPtr Config;
    const EOpType OpType;
    const TString DiskId;

    const TString NewMountToken;
    const ui64 TokenVersion;

    const ui64 FillGeneration;

    bool FallbackRequest = false;

public:
    TModifyVolumeActor(
        TRequestInfoPtr requestInfo,
        TStorageConfigPtr config,
        EOpType opType,
        TString diskId,
        TString newMountToken,
        ui64 tokenVersion,
        ui64 fillGeneration);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void TryModifyScheme(const TActorContext& ctx);

    void HandleModifySchemeResponse(
        const TEvSSProxy::TEvModifySchemeResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TModifyVolumeActor::TModifyVolumeActor(
        TRequestInfoPtr requestInfo,
        TStorageConfigPtr config,
        EOpType opType,
        TString diskId,
        TString newMountToken,
        ui64 tokenVersion,
        ui64 fillGeneration)
    : RequestInfo(std::move(requestInfo))
    , Config(std::move(config))
    , OpType(opType)
    , DiskId(std::move(diskId))
    , NewMountToken(std::move(newMountToken))
    , TokenVersion(tokenVersion)
    , FillGeneration(fillGeneration)
{}

void TModifyVolumeActor::Bootstrap(const TActorContext& ctx)
{
    TryModifyScheme(ctx);

    Become(&TThis::StateWork);
}

void TModifyVolumeActor::TryModifyScheme(const TActorContext& ctx)
{
    TString volumeDir;
    TString volumeName;

    if (!FallbackRequest) {
        std::tie(volumeDir, volumeName)  =
            DiskIdToVolumeDirAndNameDeprecated(
                Config->GetSchemeShardDir(),
                DiskId);
    } else {
        std::tie(volumeDir, volumeName) =
            DiskIdToVolumeDirAndName(
                Config->GetSchemeShardDir(),
                DiskId);
    }

    NKikimrSchemeOp::TModifyScheme modifyScheme;

    modifyScheme.SetWorkingDir(volumeDir);

    switch (OpType) {
        case EOpType::Assign: {
            modifyScheme.SetOperationType(
                NKikimrSchemeOp::ESchemeOpAssignBlockStoreVolume);

            auto* op = modifyScheme.MutableAssignBlockStoreVolume();
            op->SetName(volumeName);
            op->SetNewMountToken(NewMountToken);
            op->SetTokenVersion(TokenVersion);
            break;
        }

        case EOpType::Destroy: {
            modifyScheme.SetOperationType(
                NKikimrSchemeOp::ESchemeOpDropBlockStoreVolume);

            auto* op = modifyScheme.MutableDrop();
            op->SetName(volumeName);

            auto* opParams = modifyScheme.MutableDropBlockStoreVolume();
            opParams->SetFillGeneration(FillGeneration);

            break;
        }
    }

    auto request =
        std::make_unique<TEvSSProxy::TEvModifySchemeRequest>(modifyScheme);

    NCloud::Send(ctx, MakeSSProxyServiceId(), std::move(request));
}

void TModifyVolumeActor::HandleModifySchemeResponse(
    const TEvSSProxy::TEvModifySchemeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto& msg = *ev->Get();
    auto error = msg.GetError();

    ui32 errorCode = error.GetCode();

    // TODO: use E_NOT_FOUND instead of StatusPathDoesNotExist
    if (FAILED(errorCode) && FACILITY_FROM_CODE(errorCode) == FACILITY_SCHEMESHARD) {
        switch ((NKikimrScheme::EStatus) STATUS_FROM_CODE(errorCode)) {
            case NKikimrScheme::StatusPathDoesNotExist:
                if (!FallbackRequest) {
                    FallbackRequest = true;
                    TryModifyScheme(ctx);
                    return;
                }
                if (OpType == EOpType::Destroy) {
                    error.SetCode(S_ALREADY);
                }
                break;
            case NKikimrScheme::StatusMultipleModifications:
                error = GetErrorFromPreconditionFailed(error);
                break;
            default:
                break;
        }
    }

    const auto status = (NKikimrScheme::EStatus) msg.Status;
    const auto reason = msg.Reason;

    auto response = std::make_unique<TEvSSProxy::TEvModifyVolumeResponse>(
        error,
        msg.SchemeShardTabletId,
        status,
        reason);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

STFUNC(TModifyVolumeActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvSSProxy::TEvModifySchemeResponse, HandleModifySchemeResponse);

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

void TSSProxyActor::HandleModifyVolume(
    const TEvSSProxy::TEvModifyVolumeRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    NCloud::Register<TModifyVolumeActor>(
        ctx,
        std::move(requestInfo),
        Config,
        msg->OpType,
        msg->DiskId,
        msg->NewMountToken,
        msg->TokenVersion,
        msg->FillGeneration);
}

}   // namespace NCloud::NBlockStore::NStorage
