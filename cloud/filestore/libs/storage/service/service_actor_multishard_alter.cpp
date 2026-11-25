#include "service_actor_multishard_alter.h"

#include "service_private.h"

#include <cloud/filestore/libs/service/request.h>
#include <cloud/filestore/libs/storage/api/components.h>
#include <cloud/filestore/libs/storage/api/ss_proxy.h>
#include <cloud/filestore/libs/storage/core/config.h>

#include <cloud/storage/core/libs/actors/helpers.h>
#include <cloud/storage/core/libs/common/error.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/log.h>

#include <util/string/builder.h>

#include <memory>
#include <utility>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TMultiShardAlterActor final
    : public TActorBootstrapped<TMultiShardAlterActor>
{
private:
    constexpr static ui64 MainFileStoreCookie = Max<ui64>();

    const TStorageConfigPtr StorageConfig;
    const TActorId Owner;
    TMultiShardFileStoreConfig Config;

    ui32 Next = 0;
    ui32 InFlight = 0;
    ui32 Done = 0;

    TProtoMessagePrinter ProtoMessagePrinter;

public:
    TMultiShardAlterActor(
        TStorageConfigPtr storageConfig,
        TActorId owner,
        TMultiShardFileStoreConfig config);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void AlterFileStore(
        const TActorContext& ctx,
        NKikimrFileStore::TConfig& config,
        ui64 cookie);
    void AlterShards(const TActorContext& ctx);

    void HandleAlterFileStoreResponse(
        const TEvSSProxy::TEvAlterFileStoreResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void ReplyAndDie(const TActorContext& ctx, const NProto::TError& error);
};

////////////////////////////////////////////////////////////////////////////////

TMultiShardAlterActor::TMultiShardAlterActor(
        TStorageConfigPtr storageConfig,
        TActorId owner,
        TMultiShardFileStoreConfig config)
    : StorageConfig(std::move(storageConfig))
    , Owner(owner)
    , Config(std::move(config))
{}

void TMultiShardAlterActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    AlterFileStore(ctx, Config.MainFileSystemConfig, MainFileStoreCookie);
}

void TMultiShardAlterActor::AlterFileStore(
    const TActorContext& ctx,
    NKikimrFileStore::TConfig& config,
    ui64 cookie)
{
    config.ClearBlockSize();
    config.SetAlterTs(ctx.Now().MicroSeconds());

    LOG_INFO(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] Altering %s",
        Config.MainFileSystemConfig.GetFileSystemId().c_str(),
        config.GetFileSystemId().c_str());

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] Alter config: %s",
        Config.MainFileSystemConfig.GetFileSystemId().c_str(),
        ProtoMessagePrinter.ToString(config).c_str());

    auto request =
        std::make_unique<TEvSSProxy::TEvAlterFileStoreRequest>(config);
    NCloud::Send(ctx, MakeSSProxyServiceId(), std::move(request), cookie);
}

void TMultiShardAlterActor::AlterShards(const TActorContext& ctx)
{
    auto& shardConfigs = Config.ShardConfigs;
    if (Done == shardConfigs.size()) {
        LOG_INFO(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] Altered filesystem",
            Config.MainFileSystemConfig.GetFileSystemId().c_str());

        ReplyAndDie(ctx, {});
        return;
    }

    const ui32 limit = StorageConfig->GetMaxShardManagementRequestsInFlight();
    while (Next < shardConfigs.size() && (!limit || InFlight < limit)) {
        AlterFileStore(ctx, shardConfigs[Next], Next);
        ++Next;
        ++InFlight;
    }
}

void TMultiShardAlterActor::HandleAlterFileStoreResponse(
    const TEvSSProxy::TEvAlterFileStoreResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    if (HasError(msg->GetError())) {
        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    const auto& fileSystemId = Config.MainFileSystemConfig.GetFileSystemId();

    // The shards are altered after the filesystem.
    if (ev->Cookie == MainFileStoreCookie) {
        LOG_INFO(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] Altered %s",
            fileSystemId.c_str(),
            fileSystemId.c_str());

        AlterShards(ctx);
        return;
    }

    if (ev->Cookie >= Config.ShardConfigs.size()) {
        ReplyAndDie(
            ctx,
            MakeError(
                E_INVALID_STATE,
                TStringBuilder() << "invalid cookie: " << ev->Cookie));
        return;
    }

    LOG_INFO(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] Altered %s",
        fileSystemId.c_str(),
        Config.ShardConfigs[ev->Cookie].GetFileSystemId().c_str());

    --InFlight;
    ++Done;
    AlterShards(ctx);
}

void TMultiShardAlterActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    ReplyAndDie(ctx, MakeError(E_REJECTED, "request cancelled"));
}

void TMultiShardAlterActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    if (HasError(error)) {
        LOG_ERROR(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] Altering filesystem failed: %s",
            Config.MainFileSystemConfig.GetFileSystemId().c_str(),
            FormatError(error).c_str());
    }

    using TResponse = TEvServicePrivate::TEvMultiShardAlterResponse;
    NCloud::Send(ctx, Owner, std::make_unique<TResponse>(error));
    Die(ctx);
}

STFUNC(TMultiShardAlterActor::StateWork)
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

IActorPtr CreateMultiShardAlterActor(
    TStorageConfigPtr storageConfig,
    TActorId owner,
    TMultiShardFileStoreConfig config)
{
    return std::make_unique<TMultiShardAlterActor>(
        std::move(storageConfig),
        owner,
        std::move(config));
}

}   // namespace NCloud::NFileStore::NStorage
