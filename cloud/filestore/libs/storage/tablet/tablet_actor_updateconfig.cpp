#include "tablet_actor.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>

#include <util/string/join.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

TString ValidateUpdateConfigRequest(
    const NProto::TFileSystem& oldConfig,
    const NProto::TFileSystem& newConfig)
{
    const ui32 oldBlockSize = oldConfig.GetBlockSize();
    const ui32 newBlockSize = newConfig.GetBlockSize();

    if (oldBlockSize != newBlockSize) {
        return TStringBuilder()
            << "it's not allowed to change blockSize"
            << " (old: " << oldBlockSize
            << ", new: " << newBlockSize
            << ")";
    }

    const ui32 oldChannelCount = oldConfig.ExplicitChannelProfilesSize();
    const ui32 newChannelCount = newConfig.ExplicitChannelProfilesSize();

    if (oldChannelCount > newChannelCount) {
        return TStringBuilder()
            << "it's not allowed to decrease channelCount"
            << " (old: " << oldChannelCount
            << ", new: " << newChannelCount
            << ")";
    }

    using TChannelDiff = std::tuple<ui32, EChannelDataKind, EChannelDataKind>;
    TVector<TChannelDiff> changedChannels;

    for (ui32 c = 0; c < oldChannelCount; ++c) {
        const auto oldDataKind = static_cast<EChannelDataKind>(oldConfig
            .GetExplicitChannelProfiles(c)
            .GetDataKind());

        const auto newDataKind = static_cast<EChannelDataKind>(newConfig
            .GetExplicitChannelProfiles(c)
            .GetDataKind());

        if (oldDataKind != newDataKind) {
            changedChannels.emplace_back(c, oldDataKind, newDataKind);
        }
    }

    if (changedChannels) {
        auto error = TStringBuilder()
            << "it's not allowed to change dataKind of existing channels [";

        for (const auto& [c, oldDataKind, newDataKind]: changedChannels) {
            error << " (channel: " << c
                << ", oldDataKind: " << ToString(oldDataKind)
                << ", newDataKind: " << ToString(newDataKind)
                << ") ";
        }

        error << "]";
        return error;
    }

    if (oldChannelCount > 0) {
        // Resizing tablet: check new channels dataKind.

        using TChannelDesc = std::tuple<ui32, EChannelDataKind>;
        TVector<TChannelDesc> badNewChannels;

        for (ui32 c = oldChannelCount; c < newChannelCount; ++c) {
            const auto dataKind = static_cast<EChannelDataKind>(newConfig
                .GetExplicitChannelProfiles(c)
                .GetDataKind());

            if (dataKind != EChannelDataKind::Mixed) {
                badNewChannels.emplace_back(c, dataKind);
            }
        }

        if (badNewChannels) {
            auto error = TStringBuilder()
                << "it's allowed to add new channels with Mixed dataKind only [";

            for (const auto& [c, dataKind]: badNewChannels) {
                error << " (channel: " << c
                    << ", dataKind: " << ToString(dataKind)
                    << ") ";
            }

            error << "]";
            return error;
        }
    }

    return {};
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleUpdateConfig(
    const TEvFileStore::TEvUpdateConfig::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        // external event
        MakeIntrusive<TCallContext>(GetFileSystemId()));

    const ui64 txId = msg->Record.GetTxId();

    const auto& oldConfig = GetFileSystem();
    NProto::TFileSystem newConfig;
    Convert(msg->Record.GetConfig(), newConfig);

    if (!GetFileSystemId()) {
        LOG_INFO(ctx,TFileStoreComponents::TABLET,
            "%s Starting tablet config initialization [txId: %d]",
            LogTag.c_str(),
            txId);

        // First config update on tablet creation. No need to validate config.
        ExecuteTx<TUpdateConfig>(
            ctx,
            std::move(requestInfo),
            txId,
            std::move(newConfig));

        return;
    }

    // Setting the fields that schemeshard is not aware of.
    *newConfig.MutableFollowerFileSystemIds() =
        oldConfig.GetFollowerFileSystemIds();
    newConfig.SetShardNo(oldConfig.GetShardNo());

    // Config update occured due to alter/resize.
    if (auto error = ValidateUpdateConfigRequest(oldConfig, newConfig)) {
        LOG_ERROR(ctx, TFileStoreComponents::TABLET,
            "%s Failed to update config [txId: %lu]: %s",
            LogTag.c_str(),
            txId,
            error.c_str());

        ReportTabletUpdateConfigError();

        using TResponse = TEvFileStore::TEvUpdateConfigResponse;
        auto response = std::make_unique<TResponse>();

        response->Record.SetTxId(txId);
        response->Record.SetOrigin(TabletID());
        // do not return ERROR code: it makes schemeshard verify
        response->Record.SetStatus(NKikimrFileStore::OK);

        NCloud::Reply(ctx, *requestInfo, std::move(response));
        return;
    }

    const ui64 oldBlockCount = oldConfig.GetBlocksCount();
    const ui64 newBlockCount = newConfig.GetBlocksCount();

    if (oldBlockCount > newBlockCount) {
        LOG_WARN(ctx, TFileStoreComponents::TABLET,
            "%s BlocksCount will be decreased %lu -> %lu [txId: %lu]",
            LogTag.c_str(),
            oldBlockCount,
            newBlockCount,
            txId);
    }

    ExecuteTx<TUpdateConfig>(
        ctx,
        std::move(requestInfo),
        txId,
        std::move(newConfig));
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_UpdateConfig(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TUpdateConfig& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TIndexTabletActor::ExecuteTx_UpdateConfig(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TUpdateConfig& args)
{
    Y_UNUSED(ctx);

    TIndexTabletDatabase db(tx.DB);

    TThrottlerConfig config;
    Convert(args.FileSystem.GetPerformanceProfile(), config);

    UpdateConfig(db, args.FileSystem, config);
}

void TIndexTabletActor::CompleteTx_UpdateConfig(
    const TActorContext& ctx,
    TTxIndexTablet::TUpdateConfig& args)
{
    // update tablet id and stat counters w proper volume information
    UpdateLogTag();
    RegisterFileStore(ctx);
    RegisterStatCounters();
    ResetThrottlingPolicy();

    LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
        "%s Sending OK response for UpdateConfig with version=%u",
        LogTag.c_str(),
        args.FileSystem.GetVersion());

    auto response = std::make_unique<TEvFileStore::TEvUpdateConfigResponse>();
    response->Record.SetTxId(args.TxId);
    response->Record.SetOrigin(TabletID());
    response->Record.SetStatus(NKikimrFileStore::OK);

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleConfigureFollowers(
    const TEvIndexTablet::TEvConfigureFollowersRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        // external event
        MakeIntrusive<TCallContext>(GetFileSystemId()));

    const auto& followerIds = GetFileSystem().GetFollowerFileSystemIds();
    NProto::TError error;
    if (msg->Record.GetFollowerFileSystemIds().size() < followerIds.size()) {
        error = MakeError(E_ARGUMENT, TStringBuilder() << "new follower list"
            " is smaller than prev follower list: "
            << msg->Record.GetFollowerFileSystemIds().size() << " < "
            << followerIds.size());
    } else {
        for (int i = 0; i < followerIds.size(); ++i) {
            if (followerIds[i] != msg->Record.GetFollowerFileSystemIds(i)) {
                error = MakeError(E_ARGUMENT, TStringBuilder() << "follower"
                    " change not allowed, pos=" << i << ", prev="
                    << followerIds[i] << ", new="
                    << msg->Record.GetFollowerFileSystemIds(i));
                break;
            }
        }
    }

    if (error.GetCode() != S_OK) {
        auto response =
            std::make_unique<TEvIndexTablet::TEvConfigureFollowersResponse>();
        *response->Record.MutableError() = std::move(error);

        NCloud::Reply(ctx, *requestInfo, std::move(response));
        return;
    }

    ExecuteTx<TConfigureFollowers>(
        ctx,
        std::move(requestInfo),
        std::move(msg->Record));
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_ConfigureFollowers(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TConfigureFollowers& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TIndexTabletActor::ExecuteTx_ConfigureFollowers(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TConfigureFollowers& args)
{
    Y_UNUSED(ctx);

    TIndexTabletDatabase db(tx.DB);

    auto config = GetFileSystem();
    *config.MutableFollowerFileSystemIds() =
        std::move(*args.Request.MutableFollowerFileSystemIds());

    UpdateConfig(db, config, GetThrottlingConfig());
}

void TIndexTabletActor::CompleteTx_ConfigureFollowers(
    const TActorContext& ctx,
    TTxIndexTablet::TConfigureFollowers& args)
{
    LOG_INFO(ctx, TFileStoreComponents::TABLET,
        "%s Configured followers, new follower list: %s",
        LogTag.c_str(),
        JoinSeq(",", GetFileSystem().GetFollowerFileSystemIds()).c_str());

    auto response =
        std::make_unique<TEvIndexTablet::TEvConfigureFollowersResponse>();

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleConfigureAsFollower(
    const TEvIndexTablet::TEvConfigureAsFollowerRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        // external event
        MakeIntrusive<TCallContext>(GetFileSystemId()));

    const auto currentShardNo = GetFileSystem().GetShardNo();
    if (currentShardNo && currentShardNo != msg->Record.GetShardNo()) {
        auto response =
            std::make_unique<TEvIndexTablet::TEvConfigureAsFollowerResponse>();
        *response->Record.MutableError() = MakeError(
            E_ARGUMENT,
            TStringBuilder() << "ShardNo change not allowed: "
                << currentShardNo << " != " << msg->Record.GetShardNo());

        NCloud::Reply(ctx, *requestInfo, std::move(response));
        return;
    }

    ExecuteTx<TConfigureAsFollower>(
        ctx,
        std::move(requestInfo),
        std::move(msg->Record));
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_ConfigureAsFollower(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TConfigureAsFollower& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TIndexTabletActor::ExecuteTx_ConfigureAsFollower(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TConfigureAsFollower& args)
{
    Y_UNUSED(ctx);

    TIndexTabletDatabase db(tx.DB);

    auto config = GetFileSystem();
    config.SetShardNo(args.Request.GetShardNo());

    UpdateConfig(db, config, GetThrottlingConfig());
}

void TIndexTabletActor::CompleteTx_ConfigureAsFollower(
    const TActorContext& ctx,
    TTxIndexTablet::TConfigureAsFollower& args)
{
    LOG_INFO(ctx, TFileStoreComponents::TABLET,
        "%s Configured as follower, ShardNo: %u",
        LogTag.c_str(),
        args.Request.GetShardNo());

    auto response =
        std::make_unique<TEvIndexTablet::TEvConfigureAsFollowerResponse>();

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

}   // namespace NCloud::NFileStore::NStorage
