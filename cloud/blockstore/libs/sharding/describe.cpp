#include "describe.h"

#include "remote_storage_provider.h"

#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/diagnostics/server_stats.h>
#include <cloud/blockstore/libs/diagnostics/volume_stats.h>
#include <cloud/blockstore/libs/kikimr/helpers.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/datetime/base.h>
#include <util/system/hostname.h>

namespace NCloud::NBlockStore::NSharding {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct THostInfo {
    TString LogTag;
    IBlockStorePtr Client;
};

struct TShard {
    TAdaptiveLock Lock;
    NProto::TError FatalError;
    NProto::TError RetriableError;
    TVector<THostInfo> Hosts;
};

using TShards = THashMap<TString, TShard>;

////////////////////////////////////////////////////////////////////////////////

struct TMultiShardDescribeHandler;

struct TDescribeResponseHandler
{
    const std::shared_ptr<TMultiShardDescribeHandler> Owner;
    const TString ShardId;
    const TString LogTag;
    TShard& Shard;

    TDescribeResponseHandler(
            std::shared_ptr<TMultiShardDescribeHandler> owner,
            TString shardId,
            TString logTag);

    void operator ()(const auto& future);
};

////////////////////////////////////////////////////////////////////////////////

TFuture<NProto::TDescribeVolumeResponse> Describe(
    const IBlockStorePtr& service,
    const NProto::TDescribeVolumeRequest& request,
    bool local)
{
    auto callContext = MakeIntrusive<TCallContext>();

    auto req = std::make_shared<NProto::TDescribeVolumeRequest>();
    req->CopyFrom(request);
    if (!local) {
        req->MutableHeaders()->ClearInternal();
    }

    auto future = service->DescribeVolume(
        callContext,
        std::move(req));
    return future;
}

////////////////////////////////////////////////////////////////////////////////

struct TMultiShardDescribeHandler
    : public std::enable_shared_from_this<TMultiShardDescribeHandler>
{
    TMultiShardDescribeHandler(
            TLog log,
            TShards shards,
            NProto::TDescribeVolumeRequest request)
        : Log(std::move(log))
        , Shards(std::move(shards))
        , Request(std::move(request))
        , Promise(NewPromise<NProto::TDescribeVolumeResponse>())
    {
        for (const auto& shard: Shards) {
            Counter += shard.second.Hosts.size();
        }
    }

    void DoDescribe()
    {
        auto self = shared_from_this();
        for (auto& shard: Shards) {
            for (auto& host: shard.second.Hosts) {

                TDescribeResponseHandler handler(
                    self,
                    shard.first,
                    host.LogTag);

                if (!shard.first.empty()) {
                    STORAGE_INFO(
                        TStringBuilder()
                            << "Send remote Describe Request to " << host.LogTag
                            << " for volume " << Request.GetDiskId());
                } else {
                    STORAGE_INFO(
                        TStringBuilder()
                            << "Send local Describe Request for volume "
                            << Request.GetDiskId());
                }

                Describe(
                    host.Client,
                    Request,
                    shard.first.empty()).Subscribe(handler);
            }
        }
    }
    TLog Log;
    std::atomic<ui64> Counter{0};
    TShards Shards;
    NProto::TDescribeVolumeRequest Request;

    TAdaptiveLock Lock;
    TPromise<NProto::TDescribeVolumeResponse> Promise;
};

////////////////////////////////////////////////////////////////////////////////

TDescribeResponseHandler::TDescribeResponseHandler(
        std::shared_ptr<TMultiShardDescribeHandler> owner,
        TString shardId,
        TString logTag)
    : Owner(std::move(owner))
    , ShardId(std::move(shardId))
    , LogTag(std::move(logTag))
    , Shard(Owner->Shards[ShardId])
{}

void TDescribeResponseHandler::operator ()(const auto& future)
{
    auto& Log = Owner->Log;

    if (Owner->Promise.HasValue()) {
        return;
    }
    auto response = future.GetValue();
    if (!HasError(response)) {
        response.SetShardId(ShardId);
        with_lock(Owner->Lock) {
            if (!Owner->Promise.HasValue()) {
                Owner->Promise.SetValue(response);
            }
        }
        return;
    }

    STORAGE_DEBUG(
        TStringBuilder()
            << "Got error '" << response.GetError().GetMessage()
            << "' from " << LogTag);

    with_lock(Shard.Lock) {
        if (EErrorKind::ErrorFatal ==
            GetErrorKind(response.GetError()))
        {
            Shard.FatalError = response.GetError();
        } else {
            Shard.RetriableError = response.GetError();
        }
    }

    auto old = Owner->Counter.fetch_sub(1, std::memory_order_relaxed);
    if (old == 1) {
        TString retryShard;
        // all the shards has replied with errors.
        // try to find shard replied with retriable errors only,
        // if such shard exists the compute should retry kick
        // endpoint, otherwise volume does not exist.
        for (const auto& shard: Owner->Shards) {
            const auto& s = shard.second;
            if (!HasError(s.FatalError)) {
                *response.MutableError() =
                    std::move(s.RetriableError);
                Owner->Promise.SetValue(response);
                return;
            }
        }
        Owner->Promise.SetValue(response);
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

std::pair<TDescribeFuture, bool> DescribeRemoteVolume(
    const TString& diskId,
    const NProto::THeaders& headers,
    const IBlockStorePtr& localService,
    const IRemoteStorageProviderPtr& suProvider,
    const ILoggingServicePtr& logging,
    const NProto::TClientConfig& clientConfig)
{
    NProto::TClientAppConfig clientAppConfig;
    auto& config = *clientAppConfig.MutableClientConfig();
    config = clientConfig;
    config.SetClientId(FQDNHostName());
    auto appConfig = std::make_shared<NClient::TClientAppConfig>(clientAppConfig);

    auto shardedClients = suProvider->GetShardsClients(std::move(appConfig));

    if (shardedClients.empty()) {
        return {MakeFuture<NProto::TDescribeVolumeResponse>(), false};
    }

    NProto::TDescribeVolumeRequest request;
    request.MutableHeaders()->CopyFrom(headers);
    request.SetDiskId(diskId);

    TShards shards;

    for (const auto& shard: shardedClients) {
        TShard s;
        for (const auto& client: shard.second) {
            s.Hosts.emplace_back(
                client.GetLogTag(),
                client.GetService());
        }
        shards.emplace(shard.first, std::move(s));
    }

    TShard localShard;
    localShard.Hosts.emplace_back("local", localService);
    shards.emplace("", std::move(localShard));

    auto describeResult = std::make_shared<TMultiShardDescribeHandler>(
        logging->CreateLog("BLOCKSTORE_REMOTE_DESCRIBE"),
        std::move(shards),
        std::move(request));
    describeResult->DoDescribe();

    return {describeResult->Promise.GetFuture(), true};
}

}   // namespace NCloud::NBlockStore::NSharding
