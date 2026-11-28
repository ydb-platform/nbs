#include "service_actor_multishard_describe.h"

#include "helpers.h"
#include "service_private.h"

#include <cloud/filestore/libs/service/request.h>
#include <cloud/filestore/libs/storage/api/components.h>
#include <cloud/filestore/libs/storage/api/ss_proxy.h>
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

class TMultiShardDescribeActor final
    : public TActorBootstrapped<TMultiShardDescribeActor>
{
private:
    constexpr static ui64 MainFileStoreCookie = Max<ui64>();

    const TStorageConfigPtr StorageConfig;
    const TActorId Owner;
    const TString FileSystemId;

    TEvServicePrivate::TMultiShardDescribeResponse Response;

    TVector<TString> ShardIds;
    ui32 Next = 0;
    ui32 InFlight = 0;
    ui32 Done = 0;

    TProtoMessagePrinter ProtoMessagePrinter;

public:
    TMultiShardDescribeActor(
        TStorageConfigPtr storageConfig,
        TActorId owner,
        TString fileSystemId);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void DescribeFileStore(
        const TActorContext& ctx,
        const TString& fileSystemId,
        ui64 cookie);
    void GetShardCreationState(const TActorContext& ctx);
    void GetFileSystemTopology(const TActorContext& ctx);
    void DescribeShards(const TActorContext& ctx);

    void HandleDescribeFileStoreResponse(
        const TEvSSProxy::TEvDescribeFileStoreResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleShardCreationStateResponse(
        const TEvIndexTablet::TEvUnsafeChangeTabletStateResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleGetFileSystemTopologyResponse(
        const TEvIndexTablet::TEvGetFileSystemTopologyResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void ReplyAndDie(const TActorContext& ctx, const NProto::TError& error);
};

////////////////////////////////////////////////////////////////////////////////

TMultiShardDescribeActor::TMultiShardDescribeActor(
        TStorageConfigPtr storageConfig,
        TActorId owner,
        TString fileSystemId)
    : StorageConfig(std::move(storageConfig))
    , Owner(owner)
    , FileSystemId(std::move(fileSystemId))
{}

void TMultiShardDescribeActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    DescribeFileStore(ctx, FileSystemId, MainFileStoreCookie);
}

void TMultiShardDescribeActor::DescribeFileStore(
    const TActorContext& ctx,
    const TString& fileSystemId,
    ui64 cookie)
{
    LOG_INFO(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] Describing %s",
        FileSystemId.c_str(),
        fileSystemId.c_str());

    auto request =
        std::make_unique<TEvSSProxy::TEvDescribeFileStoreRequest>(fileSystemId);
    NCloud::Send(ctx, MakeSSProxyServiceId(), std::move(request), cookie);
}

void TMultiShardDescribeActor::HandleDescribeFileStoreResponse(
    const TEvSSProxy::TEvDescribeFileStoreResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    if (HasError(msg->GetError())) {
        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    const auto& config =
        msg->PathDescription.GetFileStoreDescription().GetConfig();
    if (ev->Cookie == MainFileStoreCookie) {
        LOG_INFO(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] Described %s",
            FileSystemId.c_str(),
            FileSystemId.c_str());

        LOG_DEBUG(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] Filesystem config: %s",
            FileSystemId.c_str(),
            ProtoMessagePrinter.ToString(config).c_str());

        Response.Config.MainFileSystemConfig = config;
        GetShardCreationState(ctx);
        return;
    }

    if (ev->Cookie >= ShardIds.size()) {
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
        "[%s] Described %s",
        FileSystemId.c_str(),
        ShardIds[ev->Cookie].c_str());

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] Shard %s config: %s",
        FileSystemId.c_str(),
        ShardIds[ev->Cookie].c_str(),
        ProtoMessagePrinter.ToString(config).c_str());

    Response.Config.ShardConfigs[ev->Cookie] = config;
    --InFlight;
    ++Done;
    DescribeShards(ctx);
}

void TMultiShardDescribeActor::GetShardCreationState(const TActorContext& ctx)
{
    // The state is saved only if shard creation is throttled.
    if (!StorageConfig->GetMaxShardManagementRequestsInFlight()) {
        GetFileSystemTopology(ctx);
        return;
    }

    LOG_INFO(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] Reading shard creation state",
        FileSystemId.c_str());

    auto request =
        std::make_unique<TEvIndexTablet::TEvUnsafeChangeTabletStateRequest>();
    request->Record.SetFileSystemId(FileSystemId);
    // Without a version the tablet only returns the state.
    request->Record.MutableShardCreationState();
    NCloud::Send(ctx, MakeIndexTabletProxyServiceId(), std::move(request));
}

void TMultiShardDescribeActor::HandleShardCreationStateResponse(
    const TEvIndexTablet::TEvUnsafeChangeTabletStateResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto& record = ev->Get()->Record;
    if (HasError(record.GetError())) {
        ReplyAndDie(
            ctx,
            MakeError(
                E_REJECTED,
                TStringBuilder()
                    << "failed to read shard creation state: "
                    << FormatError(record.GetError())));
        return;
    }

    // A tablet of the previous version returns no state.
    if (record.HasShardCreationState()) {
        Response.ShardCreationState = record.GetShardCreationState();
    }

    LOG_INFO(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] Read shard creation state",
        FileSystemId.c_str());

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] Shard creation state: %s",
        FileSystemId.c_str(),
        ProtoMessagePrinter.ToString(record).c_str());

    GetFileSystemTopology(ctx);
}

void TMultiShardDescribeActor::GetFileSystemTopology(const TActorContext& ctx)
{
    LOG_INFO(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] Getting filesystem topology",
        FileSystemId.c_str());

    auto request =
        std::make_unique<TEvIndexTablet::TEvGetFileSystemTopologyRequest>();
    request->Record.SetFileSystemId(FileSystemId);
    NCloud::Send(ctx, MakeIndexTabletProxyServiceId(), std::move(request));
}

void TMultiShardDescribeActor::HandleGetFileSystemTopologyResponse(
    const TEvIndexTablet::TEvGetFileSystemTopologyResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    if (HasError(msg->GetError())) {
        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    Response.Topology = msg->Record;

    // The topology of a shard lists its sibling shards.
    if (!Response.Topology.GetShardNo()) {
        const auto topology = GetShardTopology(Response.Topology);
        for (const auto& shard: topology.GetShards()) {
            ShardIds.push_back(shard.GetShardId());
        }
    }

    LOG_INFO(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] Got filesystem topology with %lu shards",
        FileSystemId.c_str(),
        ShardIds.size());

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] Filesystem topology: %s",
        FileSystemId.c_str(),
        ProtoMessagePrinter.ToString(Response.Topology).c_str());

    Response.Config.ShardConfigs.resize(ShardIds.size());
    DescribeShards(ctx);
}

void TMultiShardDescribeActor::DescribeShards(const TActorContext& ctx)
{
    if (Done == ShardIds.size()) {
        LOG_INFO(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] Described filesystem",
            FileSystemId.c_str());

        ReplyAndDie(ctx, {});
        return;
    }

    const ui32 limit = StorageConfig->GetMaxShardManagementRequestsInFlight();
    while (Next < ShardIds.size() && (!limit || InFlight < limit)) {
        DescribeFileStore(ctx, ShardIds[Next], Next);
        ++Next;
        ++InFlight;
    }
}

void TMultiShardDescribeActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    ReplyAndDie(ctx, MakeError(E_REJECTED, "request cancelled"));
}

void TMultiShardDescribeActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    if (HasError(error)) {
        LOG_ERROR(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] Describing filesystem failed: %s",
            FileSystemId.c_str(),
            FormatError(error).c_str());
    }

    using TResponse = TEvServicePrivate::TEvMultiShardDescribeResponse;
    auto response = std::make_unique<TResponse>(error, std::move(Response));
    NCloud::Send(ctx, Owner, std::move(response));
    Die(ctx);
}

STFUNC(TMultiShardDescribeActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(
            TEvSSProxy::TEvDescribeFileStoreResponse,
            HandleDescribeFileStoreResponse);
        HFunc(
            TEvIndexTablet::TEvUnsafeChangeTabletStateResponse,
            HandleShardCreationStateResponse);
        HFunc(
            TEvIndexTablet::TEvGetFileSystemTopologyResponse,
            HandleGetFileSystemTopologyResponse);

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

IActorPtr CreateMultiShardDescribeActor(
    TStorageConfigPtr storageConfig,
    TActorId owner,
    TString fileSystemId)
{
    return std::make_unique<TMultiShardDescribeActor>(
        std::move(storageConfig),
        owner,
        std::move(fileSystemId));
}

}   // namespace NCloud::NFileStore::NStorage
