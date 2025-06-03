#include "describe_volume.h"

#include "config.h"
#include "sharding_common.h"

#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/diagnostics/server_stats.h>
#include <cloud/blockstore/libs/diagnostics/volume_stats.h>
#include <cloud/blockstore/libs/kikimr/helpers.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/scheduler.h>
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
    const std::weak_ptr<TMultiShardDescribeHandler> Owner;
    const TString ShardId;
    const TString LogTag;
    const TFuture<NProto::TDescribeVolumeResponse> Future;
    TShard& Shard;

    TDescribeResponseHandler(
            std::weak_ptr<TMultiShardDescribeHandler> owner,
            TString shardId,
            TString logTag,
            TFuture<NProto::TDescribeVolumeResponse> future,
            TShard& shard);

    void operator ()(const auto& future);

    void Start()
    {
        Future.Subscribe(*this);
    }
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
    const ISchedulerPtr Scheduler;
    TLog Log;
    std::atomic<ui64> Counter{0};
    TShards Shards;
    NProto::TDescribeVolumeRequest Request;
    bool IncompleteShards;

    TAdaptiveLock Lock;
    bool HasValue = false;
    TPromise<NProto::TDescribeVolumeResponse> Promise;
    TVector<TDescribeResponseHandler> Handlers;

    TMultiShardDescribeHandler(
            ISchedulerPtr scheduler,
            TLog log,
            TShards shards,
            NProto::TDescribeVolumeRequest request,
            bool incompleteShards)
        : Scheduler(std::move(scheduler))
        , Log(std::move(log))
        , Shards(std::move(shards))
        , Request(std::move(request))
        , IncompleteShards(incompleteShards)
        , Promise(NewPromise<NProto::TDescribeVolumeResponse>())
    {
        for (const auto& shard: Shards) {
            Counter += shard.second.Hosts.size();
        }
    }

    void DoDescribe(TDuration describeTimeout)
    {
        auto weak = weak_from_this();
        for (auto& shard: Shards) {
            for (auto& host: shard.second.Hosts) {

                if (!shard.first.empty()) {
                    STORAGE_DEBUG(
                        TStringBuilder()
                            << "Send remote Describe Request to " << host.LogTag
                            << " for volume " << Request.GetDiskId());
                } else {
                    STORAGE_DEBUG(
                        TStringBuilder()
                            << "Send local Describe Request for volume "
                            << Request.GetDiskId());
                }

                auto future = Describe(
                    host.Client,
                    Request,
                    shard.first.empty());

                TDescribeResponseHandler handler(
                    weak,
                    shard.first,
                    host.LogTag,
                    std::move(future),
                    shard.second);

                handler.Start();
                Handlers.push_back(std::move(handler));
            }
        }

        auto self = shared_from_this();
        Scheduler->Schedule(
            TInstant::Now() + describeTimeout,
            [self=std::move(self)]() {
                self->HandleTimeout();
            });
    }

    void SetResponse(NProto::TDescribeVolumeResponse response)
    {
        with_lock(Lock) {
            if (HasValue) {
                return;
            }
            HasValue = true;
        }
        Promise.SetValue(std::move(response));
    }

    void HandleTimeout()
    {
        NProto::TDescribeVolumeResponse response;
            *response.MutableError() =
                std::move(MakeError(E_REJECTED, "Describe timeout"));
        SetResponse(std::move(response));
    }
};

////////////////////////////////////////////////////////////////////////////////

TDescribeResponseHandler::TDescribeResponseHandler(
        std::weak_ptr<TMultiShardDescribeHandler> owner,
        TString shardId,
        TString logTag,
        TFuture<NProto::TDescribeVolumeResponse> future,
        TShard& shard)
    : Owner(std::move(owner))
    , ShardId(std::move(shardId))
    , LogTag(std::move(logTag))
    , Future(std::move(future))
    , Shard(shard)
{}

void TDescribeResponseHandler::operator ()(const auto& future)
{
    auto owner = Owner.lock();
    if (!owner) {
        return;
    }
<<<<<<< HEAD

    auto& Log = owner->Log;

=======

    auto& Log = owner->Log;

>>>>>>> update & ut
    if (owner->Promise.HasValue()) {
        return;
    }
    auto response = future.GetValue();
    if (!HasError(response)) {
        response.SetShardId(ShardId);
        owner->SetResponse(std::move(response));
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

    auto now = owner->Counter.fetch_sub(1, std::memory_order_acq_rel) - 1;
    if (now == 0) {
        TString retryShard;
        // all the shards has replied with errors.
        // try to find shard replied with retriable errors only,
        // if such shard exists the compute should retry kick
        // endpoint, otherwise volume does not exist.
        for (const auto& shard: owner->Shards) {
            const auto& s = shard.second;
            if (!HasError(s.FatalError)) {
                *response.MutableError() =
                    std::move(s.RetriableError);
                owner->Promise.SetValue(response);
                return;
            }
        }
        if (owner->IncompleteShards) {
            *response.MutableError() =
                    std::move(MakeError(E_REJECTED, "Not all shards available"));
            owner->SetResponse(std::move(response));
            return;
        }
        owner->SetResponse(std::move(response));
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

std::optional<TDescribeFuture> DescribeVolume(
    const NProto::TDescribeVolumeRequest& request,
    const IBlockStorePtr& localService,
    const TShardsEndpoints& endpoints,
    bool hasUnavailableShards,
    TDuration timeout,
    TShardingArguments args)
{
    TShards shards;

    for (const auto& shard: endpoints) {
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
        args.Scheduler,
        args.Logging->CreateLog("BLOCKSTORE_SHARDING"),
        std::move(shards),
        request,
        hasUnavailableShards);
    describeResult->DoDescribe(timeout);

    return describeResult->Promise.GetFuture();
}

}   // namespace NCloud::NBlockStore::NSharding
