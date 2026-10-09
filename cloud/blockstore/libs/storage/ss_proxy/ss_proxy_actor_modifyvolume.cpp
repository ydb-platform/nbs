#include "ss_proxy_actor.h"

#include <cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/storage/model/volume_label.h>

#include <contrib/ydb/core/protos/schemeshard/operations.pb.h>
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
    const TStorageConfigConstPtr Config;
    const EOpType OpType;
    const TString DiskId;

    const TString NewMountToken;
    const ui64 TokenVersion;

    const ui64 FillGeneration;
    const TString ShardId;
    const ui64 ExpectedTabletId;
    TString VerifiedPath;
    ui64 VerifiedPathId = 0;
    ui64 VerifiedPathVersion = 0;

    bool FallbackRequest = false;

public:
    TModifyVolumeActor(TRequestInfoPtr requestInfo,
                       TStorageConfigConstPtr config, EOpType opType,
                       TString diskId, TString newMountToken, ui64 tokenVersion,
                       ui64 fillGeneration, TString shardId,
                       ui64 expectedTabletId);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void TryModifyScheme(const TActorContext& ctx);
    void HandleDescribeGuardedVolumeResponse(
        const TEvSSProxy::TEvDescribeVolumeResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleModifySchemeResponse(
        const TEvSSProxy::TEvModifySchemeResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TModifyVolumeActor::TModifyVolumeActor(
    TRequestInfoPtr requestInfo, TStorageConfigConstPtr config, EOpType opType,
    TString diskId, TString newMountToken, ui64 tokenVersion,
    ui64 fillGeneration, TString shardId, ui64 expectedTabletId)
    : RequestInfo(std::move(requestInfo))
    , Config(std::move(config))
    , OpType(opType)
    , DiskId(std::move(diskId))
    , NewMountToken(std::move(newMountToken))
    , TokenVersion(tokenVersion)
    , FillGeneration(fillGeneration)
    , ShardId(std::move(shardId))
    , ExpectedTabletId(expectedTabletId)
{}

void TModifyVolumeActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);
    if (ExpectedTabletId) {
        NCloud::Send(
            ctx, MakeSSProxyServiceId(),
            std::make_unique<TEvSSProxy::TEvDescribeVolumeRequest>(DiskId, true,
                                                                   ShardId));
    } else {
        TryModifyScheme(ctx);
    }
}

void TModifyVolumeActor::TryModifyScheme(const TActorContext& ctx)
{
    TString volumeDir;
    TString volumeName;

    if (ExpectedTabletId) {
        TStringBuf dir, name;
        TStringBuf(VerifiedPath).RSplit('/', dir, name);
        volumeDir = TString(dir);
        volumeName = TString(name);
    } else if (!FallbackRequest) {
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
            if (ExpectedTabletId) {
                op->SetId(VerifiedPathId);
                auto* condition = modifyScheme.AddApplyIf();
                condition->SetPathId(VerifiedPathId);
                condition->SetPathVersion(VerifiedPathVersion);
            }

            auto* opParams = modifyScheme.MutableDropBlockStoreVolume();
            opParams->SetFillGeneration(FillGeneration);

            break;
        }
    }

    auto request =
        std::make_unique<TEvSSProxy::TEvModifySchemeRequest>(modifyScheme);

    NCloud::Send(ctx, MakeSSProxyServiceId(), std::move(request));
}

void TModifyVolumeActor::HandleDescribeGuardedVolumeResponse(
    const TEvSSProxy::TEvDescribeVolumeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    auto error = msg->GetError();
    const auto& description = msg->PathDescription;
    if (IsNotFoundSchemeShardError(error) ||
        (!HasError(error) &&
         description.GetBlockStoreVolumeDescription().GetVolumeTabletId() !=
             ExpectedTabletId))
    {
        error = MakeError(S_ALREADY,
                          "Expected volume incarnation no longer exists");
    }
    if (HasError(error) || error.GetCode() == S_ALREADY) {
        NCloud::Reply(
            ctx, *RequestInfo,
            std::make_unique<TEvSSProxy::TEvModifyVolumeResponse>(error));
        Die(ctx);
        return;
    }
    VerifiedPath = msg->Path;
    VerifiedPathId = description.GetSelf().GetPathId();
    VerifiedPathVersion = description.GetSelf().GetPathVersion();
    if (!VerifiedPathId) {
        NCloud::Reply(
            ctx,
            *RequestInfo,
            std::make_unique<TEvSSProxy::TEvModifyVolumeResponse>(MakeError(
                E_INVALID_STATE, "Cannot verify the expected schema path")));
        Die(ctx);
        return;
    }
    TryModifyScheme(ctx);
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
                if (ExpectedTabletId) {
                    error.SetCode(S_ALREADY);
                    break;
                }
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
        HFunc(TEvSSProxy::TEvDescribeVolumeResponse,
              HandleDescribeGuardedVolumeResponse);

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

    auto config = GetConfigForShard(msg->ShardId);
    if (!config) {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvSSProxy::TEvModifyVolumeResponse>(
                MakeError(E_ARGUMENT, "Unknown or invalid storage shard")));
        return;
    }

    NCloud::Register<TModifyVolumeActor>(
        ctx, std::move(requestInfo), std::move(config), msg->OpType,
        msg->DiskId, msg->NewMountToken, msg->TokenVersion, msg->FillGeneration,
        msg->ShardId, msg->ExpectedTabletId);
}

}   // namespace NCloud::NBlockStore::NStorage
