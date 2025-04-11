#pragma once

#include "partition_info.h"
#include "tracing.h"

#include <cloud/blockstore/libs/storage/core/forward_helpers.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/storage/volume/model/merge.h>
#include <cloud/blockstore/libs/storage/volume/model/stripe.h>
#include <cloud/blockstore/libs/storage/volume/volume_events_private.h>
#include <cloud/storage/core/libs/diagnostics/trace_serializer.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/actorid.h>
#include <contrib/ydb/library/actors/core/event.h>

#include <library/cpp/eventlog/eventlog.h>
#include <library/cpp/lwtrace/all.h>

#include <memory>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
struct TPartitionRequest
{
    NActors::TActorId ActorId;
    ui64 TabletId = 0;
    ui32 PartitionId = Max<ui32>();
    TBlockRange64 BlockRange;
    std::unique_ptr<typename TMethod::TRequest> Event;
};

template <typename TMethod>
bool ToPartitionRequests(
    const TPartitionInfoList& partitions,
    const ui32 blockSize,
    const ui32 blocksPerStripe,
    const typename TMethod::TRequest::TPtr& ev,
    TVector<TPartitionRequest<TMethod>>* requests,
    TBlockRange64* blockRange);

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
class TMultiPartitionRequestActor final
    : public NActors::TActorBootstrapped<TMultiPartitionRequestActor<TMethod>>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TBlockRange64 OriginalRange;
    const ui32 BlocksPerStripe;
    const ui32 BlockSize;
    const ui32 PartitionsCount;
    TVector<TPartitionRequest<TMethod>> PartitionRequests;
    const TRequestTraceInfo TraceInfo;
    ui32 Responses = 0;
    typename TMethod::TResponse::ProtoRecordType Record;

    using TBase = NActors::TActorBootstrapped<TMultiPartitionRequestActor<TMethod>>;

    TVector<TCallContextPtr> ChildCallContexts;

public:
    TMultiPartitionRequestActor(
        TRequestInfoPtr requestInfo,
        TBlockRange64 originalRange,
        ui32 blocksPerStripe,
        ui32 blockSize,
        ui32 partitionsCount,
        TVector<TPartitionRequest<TMethod>> partitionRequests,
        TRequestTraceInfo traceInfo);

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    STFUNC(StateWork);

    void Prepare(const NActors::TActorContext& ctx);

    void HandlePartitionResponse(
        const typename TMethod::TResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleUndelivery(
        const typename TMethod::TRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    template <typename T>
    static void MergeCommonFields(const T& src, T& dst)
    {
        if (src.GetError().GetCode() > dst.GetError().GetCode()) {
            dst.MutableError()->CopyFrom(src.GetError());
        }
    }

    void Merge(
        NProto::TReadBlocksResponse& src,
        ui32 requestNo,
        NProto::TReadBlocksResponse& dst)
    {
        auto& buffers = *dst.MutableBlocks()->MutableBuffers();
        buffers.Reserve(OriginalRange.Size());
        while (static_cast<ui32>(buffers.size()) < OriginalRange.Size()) {
            buffers.Add();
        }

        auto& srcBuffers = *src.MutableBlocks()->MutableBuffers();
        for (ui32 i = 0; i < static_cast<ui32>(srcBuffers.size()); ++i) {
            const auto index = RelativeToGlobalIndex(
                BlocksPerStripe,
                PartitionRequests[requestNo].BlockRange.Start + i,
                PartitionsCount,
                PartitionRequests[requestNo].PartitionId
            );

            buffers[index - OriginalRange.Start] = std::move(srcBuffers[i]);
        }
    }

    void Merge(
        NProto::TCompactRangeResponse& src,
        ui32 requestNo,
        NProto::TCompactRangeResponse& dst)
    {
        Y_UNUSED(requestNo);

        dst.SetOperationId(src.GetOperationId());
    }

    void Merge(
        NProto::TGetCompactionStatusResponse& src,
        ui32 requestNo,
        NProto::TGetCompactionStatusResponse& dst)
    {
        Y_UNUSED(requestNo);

        dst.SetProgress(src.GetProgress() + dst.GetProgress());
        dst.SetTotal(src.GetTotal() + dst.GetTotal());
        dst.SetIsCompleted(src.GetIsCompleted() && dst.GetIsCompleted());
    }

    void Merge(
        NProto::TGetChangedBlocksResponse& src,
        ui32 requestNo,
        NProto::TGetChangedBlocksResponse& dst)
    {
        MergeStripedBitMask(
            OriginalRange,
            PartitionRequests[requestNo].BlockRange,
            BlocksPerStripe,
            PartitionsCount,
            PartitionRequests[requestNo].PartitionId,
            src.GetMask(),
            *dst.MutableMask()
        );
    }

    void Merge(
        NProto::TRebuildMetadataResponse& src,
        ui32 requestNo,
        NProto::TRebuildMetadataResponse& dst)
    {
        Y_UNUSED(requestNo);
        if (FAILED(src.GetError().GetCode()) ||
            src.GetError().GetCode() != S_OK)
        {
            *dst.MutableError() = std::move(*src.MutableError());
        }
    }

    void Merge(
        NProto::TGetRebuildMetadataStatusResponse& src,
        ui32 requestNo,
        NProto::TGetRebuildMetadataStatusResponse& dst)
    {
        Y_UNUSED(requestNo);

        if (FAILED(src.GetError().GetCode())) {
            *dst.MutableError() = std::move(*src.MutableError());
        } else {
            const auto& s = src.GetProgress();
            const auto& d = dst.GetProgress();
            bool isCompleted = Responses ? d.GetIsCompleted() : true;

            dst.MutableProgress()->SetProcessed(
                s.GetProcessed() + d.GetProcessed());
            dst.MutableProgress()->SetTotal(
                s.GetTotal() + d.GetTotal());
            dst.MutableProgress()->SetIsCompleted(
                s.GetIsCompleted() && isCompleted);
        }
    }

    void Merge(
        NProto::TScanDiskResponse& src,
        ui32 requestNo,
        NProto::TScanDiskResponse& dst)
    {
        Y_UNUSED(requestNo);
        if (FAILED(src.GetError().GetCode())) {
            *dst.MutableError() = std::move(*src.MutableError());
        }
    }

    void Merge(
        NProto::TGetScanDiskStatusResponse& src,
        ui32 requestNo,
        NProto::TGetScanDiskStatusResponse& dst)
    {
        Y_UNUSED(requestNo);

        if (FAILED(src.GetError().GetCode())) {
            *dst.MutableError() = std::move(*src.MutableError());
        } else {
            const auto& srcProgress = src.GetProgress();
            const auto& dstProgress = dst.GetProgress();

            dst.MutableProgress()->SetProcessed(
                srcProgress.GetProcessed() + dstProgress.GetProcessed());
            dst.MutableProgress()->SetTotal(
                srcProgress.GetTotal() + dstProgress.GetTotal());

            const bool dstIsCompleted = Responses
                ? dstProgress.GetIsCompleted()
                : true;
            dst.MutableProgress()->SetIsCompleted(
                srcProgress.GetIsCompleted() && dstIsCompleted);

            const auto& srcBrokenBlobs = srcProgress.GetBrokenBlobs();
            for (int i = 0; i < srcBrokenBlobs.size(); ++i) {
                auto& dstBrokenBlob = *dst.MutableProgress()->AddBrokenBlobs();
                dstBrokenBlob.SetRawX1(srcBrokenBlobs.at(i).GetRawX1());
                dstBrokenBlob.SetRawX2(srcBrokenBlobs.at(i).GetRawX2());
                dstBrokenBlob.SetRawX3(srcBrokenBlobs.at(i).GetRawX3());
            }
        }
    }

    void Merge(
        NProto::TDescribeBlocksResponse& src,
        ui32 requestNo,
        NProto::TDescribeBlocksResponse& dst)
    {
        if (FAILED(src.GetError().GetCode())) {
            *dst.MutableError() = std::move(*src.MutableError());
            return;
        }

        MergeDescribeBlocksResponse(
            src,
            dst,
            BlocksPerStripe,
            BlockSize,
            PartitionsCount,
            PartitionRequests[requestNo].PartitionId);
    }

    void Merge(
        NProto::TCheckRangeResponse& src,
        ui32 requestNo,
        NProto::TCheckRangeResponse& dst)
    {
        Y_UNUSED(requestNo);
        if (src.GetError().GetCode() > dst.GetError().GetCode()) {
            dst.MutableError()->CopyFrom(src.GetError());
        }

        if (src.GetStatus().GetCode() > dst.GetStatus().GetCode()) {
            dst.MutableStatus()->CopyFrom(src.GetStatus());
        }
    }

    template <typename T>
    void Merge(
        T& src,
        ui32 requestNo,
        T& dst)
    {
        Y_UNUSED(requestNo);
        MergeCommonFields(src, dst);
    }

    void ForkTraces(TCallContextPtr callContext)
    {
        auto& cc = RequestInfo->CallContext;
        if (!cc->LWOrbit.Fork(callContext->LWOrbit)) {
            LWTRACK(ForkFailed, cc->LWOrbit, TMethod::Name, cc->RequestId);
        }

        ChildCallContexts.push_back(callContext);
    }

    void JoinTraces(ui32 cookie)
    {
        if (cookie < ChildCallContexts.size()) {
            if (ChildCallContexts[cookie]) {
                auto& cc = RequestInfo->CallContext;
                cc->LWOrbit.Join(ChildCallContexts[cookie]->LWOrbit);
                ChildCallContexts[cookie].Reset();
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
TMultiPartitionRequestActor<TMethod>::TMultiPartitionRequestActor(
        TRequestInfoPtr requestInfo,
        TBlockRange64 originalRange,
        ui32 blocksPerStripe,
        ui32 blockSize,
        ui32 partitionsCount,
        TVector<TPartitionRequest<TMethod>> partitionRequests,
        TRequestTraceInfo traceInfo)
    : RequestInfo(std::move(requestInfo))
    , OriginalRange(originalRange)
    , BlocksPerStripe(blocksPerStripe)
    , BlockSize(blockSize)
    , PartitionsCount(partitionsCount)
    , PartitionRequests(std::move(partitionRequests))
    , TraceInfo(std::move(traceInfo))
    , ChildCallContexts(Reserve(PartitionRequests.size()))
{
    Y_DEBUG_ABORT_UNLESS(PartitionsCount >= PartitionRequests.size());
}

template <typename TMethod>
void TMultiPartitionRequestActor<TMethod>::Bootstrap(
    const NActors::TActorContext& ctx)
{
    Prepare(ctx);


    ui32 requestNo = 0;
    for (auto& partitionRequest: PartitionRequests) {
        const auto selfId = TBase::SelfId();

        ForkTraces(partitionRequest.Event->CallContext);

        auto event = std::make_unique<NActors::IEventHandle>(
            partitionRequest.ActorId,
            selfId,
            partitionRequest.Event.release(),
            NActors::IEventHandle::FlagForwardOnNondelivery,
            requestNo++,
            &selfId);

        ctx.Send(event.release());
    }

    TBase::Become(&TBase::TThis::StateWork);
}

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
void TMultiPartitionRequestActor<TMethod>::Prepare(
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ctx);
    if constexpr(std::is_same_v<TMethod, TEvVolume::TGetCompactionStatusMethod>) {
        Record.SetIsCompleted(true);
    }
}

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
void TMultiPartitionRequestActor<TMethod>::HandlePartitionResponse(
    const typename TMethod::TResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();

    const ui32 requestNo = ev->Cookie;

    if (FAILED(msg->GetStatus())) {
        Record.MutableError()->CopyFrom(msg->GetError());
    } else {
        Merge(msg->Record, requestNo, Record);
    }

    JoinTraces(requestNo);

    if (++Responses == PartitionRequests.size() || FAILED(msg->GetStatus())) {
        auto response = std::make_unique<typename TMethod::TResponse>(
            Record
        );

        if (TraceInfo.IsTraced) {
            TraceInfo.TraceSerializer->BuildTraceInfo(
                *response->Record.MutableTrace(),
                RequestInfo->CallContext->LWOrbit,
                TraceInfo.ReceiveTime,
                GetCycleCount());
        }

        LWTRACK(
            ResponseSent_Volume,
            RequestInfo->CallContext->LWOrbit,
            TMethod::Name,
            RequestInfo->CallContext->RequestId);

        NCloud::Reply(ctx, *RequestInfo, std::move(response));

        TBase::Die(ctx);
    }
}

template <typename TMethod>
void TMultiPartitionRequestActor<TMethod>::HandleUndelivery(
    const typename TMethod::TRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ev);

    *Record.MutableError() = MakeError(
        E_REJECTED,
        "failed to deliver request to some partitions"
    );

    auto response = std::make_unique<typename TMethod::TResponse>(
        Record
    );

    LWTRACK(
        ResponseSent_Volume,
        RequestInfo->CallContext->LWOrbit,
        TMethod::Name,
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));

    TBase::Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
STFUNC(TMultiPartitionRequestActor<TMethod>::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TMethod::TResponse, HandlePartitionResponse);
        HFunc(TMethod::TRequest, HandleUndelivery);

        default:
            HandleUnexpectedEvent(ev, TBlockStoreComponents::VOLUME);
            break;
    }
}

}   // namespace NCloud::NBlockStore::NStorage
