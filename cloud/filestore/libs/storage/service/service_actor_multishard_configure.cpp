#include "service_actor_multishard_configure.h"

#include "service_private.h"

#include <cloud/filestore/libs/service/request.h>
#include <cloud/filestore/libs/storage/api/components.h>
#include <cloud/filestore/libs/storage/api/tablet.h>
#include <cloud/filestore/libs/storage/api/tablet_proxy.h>
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

class TMultiShardConfigureActor final
    : public TActorBootstrapped<TMultiShardConfigureActor>
{
private:
    const TStorageConfigPtr StorageConfig;
    const TActorId Owner;
    const TMultiShardConfigureConfig Config;

    // The shards are configured from the last one to the first: the new
    // shards before the existing ones. The shards from Next on are sent.
    ui32 Next = -1;
    ui32 InFlight = 0;
    ui32 Done = 0;

    TProtoMessagePrinter ProtoMessagePrinter;

public:
    TMultiShardConfigureActor(
        TStorageConfigPtr storageConfig,
        TActorId owner,
        TMultiShardConfigureConfig config);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void ConfigureShards(const TActorContext& ctx);
    void ConfigureAsShard(const TActorContext& ctx, ui32 shardIndex);
    void ConfigureMainFileStore(const TActorContext& ctx);

    void HandleConfigureAsShardResponse(
        const TEvIndexTablet::TEvConfigureAsShardResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleConfigureShardsResponse(
        const TEvIndexTablet::TEvConfigureShardsResponse::TPtr& ev,
        const TActorContext& ctx);

    const TString& GetShardId(ui32 shardIndex) const;

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void ReplyAndDie(const TActorContext& ctx, const NProto::TError& error);
};

////////////////////////////////////////////////////////////////////////////////

TMultiShardConfigureActor::TMultiShardConfigureActor(
        TStorageConfigPtr storageConfig,
        TActorId owner,
        TMultiShardConfigureConfig config)
    : StorageConfig(std::move(storageConfig))
    , Owner(owner)
    , Config(std::move(config))
    , Next(Config.Topology.ShardsSize())
{}

void TMultiShardConfigureActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    ConfigureShards(ctx);
}

void TMultiShardConfigureActor::ConfigureShards(const TActorContext& ctx)
{
    const ui32 shardCount = Config.Topology.ShardsSize();
    if (Done == shardCount) {
        ConfigureMainFileStore(ctx);
        return;
    }

    // An existing shard gets the shard list only after all the new shards are
    // configured: it can create nodes in them right away.
    ui32 begin = Config.ExistingShardCount;
    if (Done >= shardCount - Config.ExistingShardCount) {
        begin = 0;
    }

    const ui32 limit = StorageConfig->GetMaxShardManagementRequestsInFlight();
    while (Next > begin && (!limit || InFlight < limit)) {
        --Next;
        ++InFlight;
        ConfigureAsShard(ctx, Next);
    }
}

void TMultiShardConfigureActor::ConfigureAsShard(
    const TActorContext& ctx,
    ui32 shardIndex)
{
    auto request = std::make_unique<TEvIndexTablet::TEvConfigureAsShardRequest>();
    auto& record = request->Record;
    record.SetFileSystemId(GetShardId(shardIndex));
    record.SetShardNo(shardIndex + 1);
    record.SetMainFileSystemId(Config.FileSystemId);
    record.SetDirectoryCreationInShardsEnabled(
        Config.DirectoryCreationInShardsEnabled);
    record.SetForceDirectoryCreationInShards(
        Config.ForceDirectoryCreationInShards);
    record.SetStrictFileSystemSizeEnforcementEnabled(
        Config.StrictFileSystemSizeEnforcementEnabled);

    if (Config.DirectoryCreationInShardsEnabled ||
        Config.StrictFileSystemSizeEnforcementEnabled)
    {
        for (const auto& other: Config.Topology.GetShards()) {
            record.AddShardFileSystemIds(other.GetShardId());
        }

        *record.MutableShardTopology() = Config.Topology;
    }

    const auto& shard = Config.Topology.GetShards(shardIndex);
    if (shard.GetType() == NProtoPrivate::TShardTopology::E_FASTSHARD) {
        record.SetIsFastShard(true);
        *record.MutableFastShardConfig() = shard.GetFastShardConfig();
    }

    LOG_INFO(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] Configuring shard %s",
        Config.FileSystemId.c_str(),
        GetShardId(shardIndex).c_str());

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] ConfigureAsShard request: %s",
        Config.FileSystemId.c_str(),
        ProtoMessagePrinter.ToString(record).c_str());

    NCloud::Send(
        ctx,
        MakeIndexTabletProxyServiceId(),
        std::move(request),
        shardIndex   // cookie
    );
}

void TMultiShardConfigureActor::HandleConfigureAsShardResponse(
    const TEvIndexTablet::TEvConfigureAsShardResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    if (HasError(msg->GetError())) {
        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    if (ev->Cookie >= Config.Topology.ShardsSize()) {
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
        "[%s] Configured shard %s",
        Config.FileSystemId.c_str(),
        GetShardId(ev->Cookie).c_str());

    --InFlight;
    ++Done;
    ConfigureShards(ctx);
}

void TMultiShardConfigureActor::ConfigureMainFileStore(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvIndexTablet::TEvConfigureShardsRequest>();
    auto& record = request->Record;
    record.SetFileSystemId(Config.FileSystemId);
    record.SetDirectoryCreationInShardsEnabled(
        Config.DirectoryCreationInShardsEnabled);
    record.SetForceDirectoryCreationInShards(
        Config.ForceDirectoryCreationInShards);
    record.SetStrictFileSystemSizeEnforcementEnabled(
        Config.StrictFileSystemSizeEnforcementEnabled);
    for (const auto& shard: Config.Topology.GetShards()) {
        record.AddShardFileSystemIds(shard.GetShardId());
    }
    *record.MutableShardTopology() = Config.Topology;

    LOG_INFO(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] Configuring main filesystem with %d shards",
        Config.FileSystemId.c_str(),
        Config.Topology.ShardsSize());

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] ConfigureShards request: %s",
        Config.FileSystemId.c_str(),
        ProtoMessagePrinter.ToString(record).c_str());

    NCloud::Send(ctx, MakeIndexTabletProxyServiceId(), std::move(request));
}

void TMultiShardConfigureActor::HandleConfigureShardsResponse(
    const TEvIndexTablet::TEvConfigureShardsResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    if (!HasError(msg->GetError())) {
        LOG_INFO(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] Configured main filesystem",
            Config.FileSystemId.c_str());
    }

    ReplyAndDie(ctx, msg->GetError());
}

const TString& TMultiShardConfigureActor::GetShardId(ui32 shardIndex) const
{
    return Config.Topology.GetShards(shardIndex).GetShardId();
}

void TMultiShardConfigureActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    ReplyAndDie(ctx, MakeError(E_REJECTED, "request cancelled"));
}

void TMultiShardConfigureActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    if (HasError(error)) {
        LOG_ERROR(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] Configuring shards failed: %s",
            Config.FileSystemId.c_str(),
            FormatError(error).c_str());
    }

    using TResponse = TEvServicePrivate::TEvMultiShardConfigureResponse;
    NCloud::Send(ctx, Owner, std::make_unique<TResponse>(error));
    Die(ctx);
}

STFUNC(TMultiShardConfigureActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(
            TEvIndexTablet::TEvConfigureAsShardResponse,
            HandleConfigureAsShardResponse);
        HFunc(
            TEvIndexTablet::TEvConfigureShardsResponse,
            HandleConfigureShardsResponse);

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

IActorPtr CreateMultiShardConfigureActor(
    TStorageConfigPtr storageConfig,
    TActorId owner,
    TMultiShardConfigureConfig config)
{
    return std::make_unique<TMultiShardConfigureActor>(
        std::move(storageConfig),
        owner,
        std::move(config));
}

}   // namespace NCloud::NFileStore::NStorage
