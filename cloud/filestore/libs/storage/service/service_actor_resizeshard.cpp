#include "service_actor_resizeshard.h"

#include "helpers.h"
#include "service_actor.h"

#include <cloud/filestore/libs/storage/api/ss_proxy.h>
#include <cloud/filestore/libs/storage/core/model.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TResizeShardActor final
    : public TActorBootstrapped<TResizeShardActor>
{
private:
    const TStorageConfigPtr StorageConfig;
    const TRequestInfoPtr RequestInfo;
    const NProto::TResizeFileStoreRequest Request;
    const ui32 ShardNo;
    const bool StrictFileSystemSizeEnforcementEnabled;

    NKikimrFileStore::TConfig Config;

public:
    TResizeShardActor(
        TStorageConfigPtr storageConfig,
        TRequestInfoPtr requestInfo,
        NProto::TResizeFileStoreRequest request,
        NKikimrFileStore::TConfig config,
        const NProtoPrivate::TGetFileSystemTopologyResponse& topology);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void HandleAlterFileStoreResponse(
        const TEvSSProxy::TEvAlterFileStoreResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void ReplyAndDie(const TActorContext& ctx, const NProto::TError& error);
};

////////////////////////////////////////////////////////////////////////////////

TResizeShardActor::TResizeShardActor(
        TStorageConfigPtr storageConfig,
        TRequestInfoPtr requestInfo,
        NProto::TResizeFileStoreRequest request,
        NKikimrFileStore::TConfig config,
        const NProtoPrivate::TGetFileSystemTopologyResponse& topology)
    : StorageConfig(std::move(storageConfig))
    , RequestInfo(std::move(requestInfo))
    , Request(std::move(request))
    , ShardNo(topology.GetShardNo())
    , StrictFileSystemSizeEnforcementEnabled(
          Request.GetEnableStrictFileSystemSizeEnforcement() ||
          topology.GetStrictFileSystemSizeEnforcementEnabled())
    , Config(std::move(config))
{}

void TResizeShardActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    LOG_WARN(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] GetFileSystemTopology - resized a shard (%u), no subshards"
        " will be added",
        Request.GetFileSystemId().c_str(),
        ShardNo);

    if (Config.GetBlocksCount() > Request.GetBlocksCount() &&
        !Request.GetForce())
    {
        ReplyAndDie(
            ctx,
            MakeError(E_ARGUMENT, "Cannot decrease filestore size"));
        return;
    }

    if (StrictFileSystemSizeEnforcementEnabled) {
        ReplyAndDie(
            ctx,
            MakeError(
                E_ARGUMENT,
                "Cannot change shard size if "
                "'StrictFileSystemSizeEnforcementEnabled'"));
        return;
    }

    const bool allocateMixed0 = HasMixed0Channel(Config);
    Config.SetBlocksCount(Request.GetBlocksCount());
    SetupFileStorePerformanceAndChannels(
        allocateMixed0,
        *StorageConfig,
        Config,
        Request.GetPerformanceProfile());

    if (const auto version = Request.GetConfigVersion()) {
        Config.SetVersion(version);
    }

    Config.ClearBlockSize();
    Config.SetAlterTs(ctx.Now().MicroSeconds());

    auto request =
        std::make_unique<TEvSSProxy::TEvAlterFileStoreRequest>(Config);
    NCloud::Send(ctx, MakeSSProxyServiceId(), std::move(request));
}

void TResizeShardActor::HandleAlterFileStoreResponse(
    const TEvSSProxy::TEvAlterFileStoreResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (HasError(msg->GetError())) {
        LOG_ERROR(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] Altering of shard failed: %s",
            Request.GetFileSystemId().c_str(),
            FormatError(msg->GetError()).Quote().c_str());
    }

    ReplyAndDie(ctx, msg->GetError());
}

void TResizeShardActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    ReplyAndDie(ctx, MakeError(E_REJECTED, "request cancelled"));
}

void TResizeShardActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    auto response =
        std::make_unique<TEvService::TEvResizeFileStoreResponse>(error);
    NCloud::Reply(ctx, *RequestInfo, std::move(response));

    Die(ctx);
}

STFUNC(TResizeShardActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(
            TEvSSProxy::TEvAlterFileStoreResponse,
            HandleAlterFileStoreResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TFileStoreComponents::SERVICE_WORKER,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IActorPtr CreateResizeShardActor(
    TStorageConfigPtr storageConfig,
    TRequestInfoPtr requestInfo,
    NProto::TResizeFileStoreRequest request,
    NKikimrFileStore::TConfig config,
    const NProtoPrivate::TGetFileSystemTopologyResponse& topology)
{
    return std::make_unique<TResizeShardActor>(
        std::move(storageConfig),
        std::move(requestInfo),
        std::move(request),
        std::move(config),
        topology);
}

}   // namespace NCloud::NFileStore::NStorage
