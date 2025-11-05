#include "service_actor.h"

#include <cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/mount_token.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

#include <util/generic/hash.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TAssignVolumeActor final
    : public TActorBootstrapped<TAssignVolumeActor>
{
private:
    const TActorId Sender;
    const ui64 Cookie;

    const TStorageConfigPtr Config;
    const TString DiskId;
    const TMountToken PublicToken;
    const ui64 TokenVersion;

public:
    TAssignVolumeActor(
        const TActorId& sender,
        ui64 cookie,
        TStorageConfigPtr config,
        TString diskId,
        TMountToken publicToken,
        ui64 tokenVersion);

    void Bootstrap(const TActorContext& ctx);

private:
    void AssignVolume(const TActorContext& ctx);

    void HandleModifyResponse(
        const TEvSSProxy::TEvModifyVolumeResponse::TPtr& ev,
        const TActorContext& ctx);

    void ReplyAndDie(
        const TActorContext& ctx,
        std::unique_ptr<TEvService::TEvAssignVolumeResponse> response);

private:
    STFUNC(StateWork);
};

////////////////////////////////////////////////////////////////////////////////

TAssignVolumeActor::TAssignVolumeActor(
        const TActorId& sender,
        ui64 cookie,
        TStorageConfigPtr config,
        TString diskId,
        TMountToken publicToken,
        ui64 tokenVersion)
    : Sender(sender)
    , Cookie(cookie)
    , Config(std::move(config))
    , DiskId(std::move(diskId))
    , PublicToken(std::move(publicToken))
    , TokenVersion(tokenVersion)
{}

void TAssignVolumeActor::Bootstrap(const TActorContext& ctx)
{
    AssignVolume(ctx);
    Become(&TThis::StateWork);
}

void TAssignVolumeActor::AssignVolume(const TActorContext& ctx)
{
    NCloud::Send(
        ctx,
        MakeSSProxyServiceId(),
        std::make_unique<TEvSSProxy::TEvModifyVolumeRequest>(
            TEvSSProxy::TModifyVolumeRequest::EOpType::Assign,
            DiskId,
            PublicToken.ToString(),
            TokenVersion
        ));
}

void TAssignVolumeActor::HandleModifyResponse(
    const TEvSSProxy::TEvModifyVolumeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (FAILED(msg->GetError().GetCode())) {
        LOG_DEBUG(ctx, TBlockStoreComponents::SERVICE,
            "Volume %s: assign failed",
            DiskId.Quote().data());

        ReplyAndDie(
            ctx,
            std::make_unique<TEvService::TEvAssignVolumeResponse>(
                msg->GetError()));
        return;
    }

    LOG_DEBUG(ctx, TBlockStoreComponents::SERVICE,
        "Volume %s assigned successfully",
        DiskId.Quote().data());

    ReplyAndDie(
        ctx,
        std::make_unique<TEvService::TEvAssignVolumeResponse>());
}

void TAssignVolumeActor::ReplyAndDie(
    const TActorContext& ctx,
    std::unique_ptr<TEvService::TEvAssignVolumeResponse> response)
{
    NCloud::Send(ctx, Sender, std::move(response), Cookie);
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TAssignVolumeActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvSSProxy::TEvModifyVolumeResponse, HandleModifyResponse);

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

void TServiceActor::HandleAssignVolume(
    const TEvService::TEvAssignVolumeRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto& request = msg->Record;
    const auto& diskId = request.GetDiskId();
    const auto tokenVersion = request.GetTokenVersion();
    const auto& mountSecret = request.GetToken();

    TMountToken publicToken;
    if (!mountSecret.empty()) {
        publicToken.SetSecret(TMountToken::EFormat::SHA384_V1, mountSecret);
    }

    LOG_INFO(ctx, TBlockStoreComponents::SERVICE,
        "Assign volume: %s, %s, %s, %s, %llu",
        diskId.Quote().c_str(),
        request.GetInstanceId().Quote().c_str(),
        publicToken.ToString().c_str(),
        request.GetHost().Quote().c_str(),
        tokenVersion);

    NCloud::Register<TAssignVolumeActor>(
        ctx,
        ev->Sender,
        ev->Cookie,
        Config,
        diskId,
        std::move(publicToken),
        tokenVersion);
}

}   // namespace NCloud::NBlockStore::NStorage
