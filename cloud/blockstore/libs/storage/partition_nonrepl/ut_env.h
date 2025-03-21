#pragma once

#include "config.h"
#include "part_nonrepl_events_private.h"

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/storage/api/disk_registry.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/stats_service.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/storage/stats_service/stats_service_events_private.h>
#include <cloud/blockstore/libs/storage/volume/volume_events_private.h>
#include <cloud/storage/core/libs/common/sglist_test.h>
#include <cloud/storage/core/libs/kikimr/helpers.h>

#include <contrib/ydb/core/testlib/basics/runtime.h>
#include <contrib/ydb/library/actors/core/log.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/base.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

const auto WaitTimeout = TDuration::Seconds(5);
const ui64 DefaultDeviceBlockSize = 512;

////////////////////////////////////////////////////////////////////////////////

struct TStorageStatsServiceState: TAtomicRefCount<TStorageStatsServiceState>
{
    TPartitionDiskCounters Counters{
        EPublishingPolicy::DiskRegistryBased,
        EHistogramCounterOption::ReportMultipleCounters};
    TPartitionDiskCounters AggregatedCounters{
        EPublishingPolicy::DiskRegistryBased,
        EHistogramCounterOption::ReportMultipleCounters};
};

using TStorageStatsServiceStatePtr = TIntrusivePtr<TStorageStatsServiceState>;

////////////////////////////////////////////////////////////////////////////////

class TStorageStatsServiceMock final
    : public NActors::TActor<TStorageStatsServiceMock>
{
private:
    TStorageStatsServiceStatePtr State;

public:
    TStorageStatsServiceMock(TStorageStatsServiceStatePtr state)
        : TActor(&TThis::StateWork)
        , State(std::move(state))
    {
    }

private:
    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(NActors::TEvents::TEvPoisonPill, HandlePoisonPill);

            HFunc(TEvStatsService::TEvVolumePartCounters, HandleVolumePartCounters);

            HFunc(
                TEvStatsServicePrivate::TEvRegisterTrafficSourceRequest,
                HandleRegisterTrafficSource);

            default:
                Y_ABORT("Unexpected event %x", ev->GetTypeRewrite());
        }
    }

    void HandlePoisonPill(
        const NActors::TEvents::TEvPoisonPill::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        Y_UNUSED(ev);

        Die(ctx);
    }

    void HandleVolumePartCounters(
        const TEvStatsService::TEvVolumePartCounters::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        Y_UNUSED(ctx);

        State->Counters = *ev->Get()->DiskCounters;
        State->AggregatedCounters.AggregateWith(*ev->Get()->DiskCounters);
    }

    void HandleRegisterTrafficSource(
        const TEvStatsServicePrivate::TEvRegisterTrafficSourceRequest::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        auto* msg = ev->Get();

        auto response = std::make_unique<
            TEvStatsServicePrivate::TEvRegisterTrafficSourceResponse>(
            msg->BandwidthMiBs);

        NCloud::Reply(ctx, *ev, std::move(response));
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TMigrationState
{
    bool IsMigrationAllowed = true;
};

using TMigrationStatePtr = std::shared_ptr<TMigrationState>;

////////////////////////////////////////////////////////////////////////////////

class TDummyActor final
    : public NActors::TActor<TDummyActor>
{
private:
    TMigrationStatePtr MigrationState;

public:
    TDummyActor(TMigrationStatePtr migrationState = nullptr)
        : TActor(&TThis::StateWork)
        , MigrationState(std::move(migrationState))
    {
    }

private:
    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(NActors::TEvents::TEvPoisonPill, HandlePoisonPill);

            HFunc(
                TEvVolume::TEvDiskRegistryBasedPartitionCounters,
                HandleVolumePartCounters);

            HFunc(TEvVolume::TEvRdmaUnavailable, HandleRdmaUnavailable);

            HFunc(TEvVolume::TEvUpdateResyncState, HandleUpdateResyncState);

            HFunc(TEvVolume::TEvResyncFinished, HandleResyncFinished);

            HFunc(TEvDiskRegistry::TEvFinishMigrationRequest, HandleFinishMigration);

            HFunc(TEvVolume::TEvPreparePartitionMigrationRequest, HandlePreparePartitionMigration);
            HFunc(TEvVolume::TEvUpdateMigrationState, HandleUpdateMigrationState);

            IgnoreFunc(TEvVolumePrivate::TEvLaggingAgentMigrationFinished);
            IgnoreFunc(TEvVolumePrivate::TEvDeviceTimeoutedRequest);

            default:
                Y_ABORT("Unexpected event %x", ev->GetTypeRewrite());
        }
    }

    void HandlePoisonPill(
        const NActors::TEvents::TEvPoisonPill::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        Y_UNUSED(ev);

        Die(ctx);
    }

    void HandleVolumePartCounters(
        const TEvVolume::TEvDiskRegistryBasedPartitionCounters::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        auto event = std::make_unique<NActors::IEventHandle>(
            MakeStorageStatsServiceId(),
            ev->Sender,
            new TEvStatsService::TEvVolumePartCounters(
                "", // diskId
                std::move(ev->Get()->DiskCounters),
                0,
                0,
                false,
                NBlobMetrics::TBlobLoadMetrics()),
            ev->Flags,
            ev->Cookie);
        ctx.Send(event.release());
    }

    void HandleRdmaUnavailable(
        const TEvVolume::TEvRdmaUnavailable::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        Y_UNUSED(ev);
        Y_UNUSED(ctx);
    }

    void HandleUpdateResyncState(
        const TEvVolume::TEvUpdateResyncState::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        NCloud::Reply(ctx, *ev,
            std::make_unique<TEvVolume::TEvResyncStateUpdated>());
    }

    void HandleResyncFinished(
        const TEvVolume::TEvResyncFinished::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        Y_UNUSED(ev);
        Y_UNUSED(ctx);
    }

    void HandleFinishMigration(
        const TEvDiskRegistry::TEvFinishMigrationRequest::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        NCloud::Reply(ctx, *ev,
            std::make_unique<TEvDiskRegistry::TEvFinishMigrationResponse>());
    }

    void HandlePreparePartitionMigration(
        const TEvVolume::TEvPreparePartitionMigrationRequest::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvVolume::TEvPreparePartitionMigrationResponse>(
                MigrationState ? MigrationState->IsMigrationAllowed : true));
    }

    void HandleUpdateMigrationState(
        const TEvVolume::TEvUpdateMigrationState::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvVolume::TEvMigrationStateUpdated>());
    }
};

////////////////////////////////////////////////////////////////////////////////

class TStorageServiceMock final: public NActors::TActor<TStorageServiceMock>
{
public:
    TStorageServiceMock()
        : TActor(&TThis::StateWork)
    {
    }

private:
    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(NActors::TEvents::TEvPoisonPill, HandlePoisonPill);

            HFunc(TEvService::TEvAddTagsRequest, HandleAddTagsRequest);

            default:
                Y_ABORT("Unexpected event %x", ev->GetTypeRewrite());
        }
    }

    void HandlePoisonPill(
        const NActors::TEvents::TEvPoisonPill::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        Y_UNUSED(ev);

        Die(ctx);
    }

    void HandleAddTagsRequest(
        const TEvService::TEvAddTagsRequest::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        auto* msg = ev->Get();

        auto requestInfo =
            CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

        auto response =
            std::make_unique<TEvService::TEvAddTagsResponse>();

        NCloud::Reply(ctx, *requestInfo, std::move(response));
    }
};

////////////////////////////////////////////////////////////////////////////////

class TPartitionClient
{
private:
    NActors::TTestActorRuntime& Runtime;
    ui32 NodeIdx;
    NActors::TActorId ActorId;
    NActors::TActorId Sender;
    const ui32 BlockSize;
    ui64 RequestId = 0;

public:
    TPartitionClient(
            NActors::TTestActorRuntime& runtime,
            NActors::TActorId actorId,
            ui32 blockSize = DefaultBlockSize)
        : Runtime(runtime)
        , NodeIdx(0)
        , ActorId(actorId)
        , Sender(runtime.AllocateEdgeActor())
        , BlockSize(blockSize)
    {}

    const NActors::TActorId& GetActorId() const
    {
        return ActorId;
    }

    template <typename TRequest>
    void SendRequest(
        const NActors::TActorId& recipient,
        std::unique_ptr<TRequest> request,
        ui64 cookie = 0)
    {
        auto* ev = new NActors::IEventHandle(
            recipient,
            Sender,
            request.release(),
            0,          // flags
            cookie);

        Runtime.Send(ev, NodeIdx);
    }

    template <typename TResponse>
    std::unique_ptr<TResponse> RecvResponse(ui64 cookie = 0)
    {
        TAutoPtr<NActors::IEventHandle> handle;
        Runtime.GrabEdgeEventRethrow<TResponse>(handle, WaitTimeout);

        UNIT_ASSERT(handle);
        if (cookie) {
            UNIT_ASSERT_VALUES_EQUAL(cookie, handle->Cookie);
        }
        return std::unique_ptr<TResponse>(handle->Release<TResponse>().Release());
    }

    auto CreateReadBlocksRequest(
        const TBlockRange64& blockRange,
        ui32 replicaIndex = 0,
        ui32 replicaCount = 0)
    {
        auto request = std::make_unique<TEvService::TEvReadBlocksRequest>();
        request->Record.SetStartIndex(blockRange.Start);
        request->Record.SetBlocksCount(blockRange.Size());
        if (replicaIndex) {
            request->Record.MutableHeaders()->SetReplicaIndex(replicaIndex);
        }
        if (replicaCount) {
            request->Record.MutableHeaders()->SetReplicaCount(replicaCount);
        }

        return request;
    }

    auto CreateWriteBlocksRequest(
        const TBlockRange64& blockRange,
        char fill,
        ui32 blocksInBuffer = 1)
    {
        auto request = std::make_unique<TEvService::TEvWriteBlocksRequest>();

        request->Record.SetStartIndex(blockRange.Start);
        for (ui32 i = 0; i < blockRange.Size(); i += blocksInBuffer) {
            auto& b = *request->Record.MutableBlocks()->AddBuffers();
            b.resize(Min<ui32>(
                blocksInBuffer,
                (blockRange.Size() - i)
            ) * BlockSize, fill);
        }

        return request;
    }

    auto CreateZeroBlocksRequest(const TBlockRange64& blockRange)
    {
        auto request = std::make_unique<TEvService::TEvZeroBlocksRequest>();
        request->Record.SetStartIndex(blockRange.Start);
        request->Record.SetBlocksCount(blockRange.Size());

        return request;
    }

    auto CreateWriteBlocksLocalRequest(TBlockRange64 range, const TString& content)
    {
        TSgList sglist(range.Size(), TBlockDataRef{ content.data(), content.size() });

        auto request = std::make_unique<TEvService::TEvWriteBlocksLocalRequest>();
        request->Record.Sglist = TGuardedSgList(std::move(sglist));
        request->Record.SetStartIndex(range.Start);
        request->Record.BlocksCount = range.Size();
        request->Record.BlockSize = BlockSize;
        Y_ABORT_UNLESS(BlockSize == content.size());

        return request;
    }

    auto CreateReadBlocksLocalRequest(TBlockRange64 range, TGuardedSgList sglist)
    {
        auto request = std::make_unique<TEvService::TEvReadBlocksLocalRequest>();
        request->Record.Sglist = std::move(sglist);
        request->Record.SetStartIndex(range.Start);
        request->Record.SetBlocksCount(range.Size());
        request->Record.BlockSize = BlockSize;

        return request;
    }

    auto CreateChecksumBlocksRequest(TBlockRange64 range)
    {
        auto request = std::make_unique<TEvNonreplPartitionPrivate::TEvChecksumBlocksRequest>();
        request->Record.SetStartIndex(range.Start);
        request->Record.SetBlocksCount(range.Size());
        request->Record.SetBlockSize(BlockSize);

        return request;
    }

    std::unique_ptr<TEvVolume::TEvCheckRangeRequest>
    CreateCheckRangeRequest(TString id, ui32 startIndex, ui32 size, bool calculateChecksums = false)
    {
        auto request = std::make_unique<TEvVolume::TEvCheckRangeRequest>();
        request->Record.SetDiskId(id);
        request->Record.SetStartIndex(startIndex);
        request->Record.SetBlocksCount(size);
        request->Record.SetCalculateChecksums(calculateChecksums);
        return request;
    }


#define BLOCKSTORE_DECLARE_METHOD(name, ns)                                    \
    template <typename... Args>                                                \
    void Send##name##Request(Args&&... args)                                   \
    {                                                                          \
        auto request = Create##name##Request(std::forward<Args>(args)...);     \
        SendRequest(ActorId, std::move(request), ++RequestId);                 \
    }                                                                          \
                                                                               \
    std::unique_ptr<ns::TEv##name##Response> Recv##name##Response()            \
    {                                                                          \
        return RecvResponse<ns::TEv##name##Response>();                        \
    }                                                                          \
                                                                               \
    template <typename... Args>                                                \
    std::unique_ptr<ns::TEv##name##Response> name(Args&&... args)              \
    {                                                                          \
        auto request = Create##name##Request(std::forward<Args>(args)...);     \
        SendRequest(ActorId, std::move(request), ++RequestId);                 \
                                                                               \
        auto response = RecvResponse<ns::TEv##name##Response>();               \
        UNIT_ASSERT_C(                                                         \
            SUCCEEDED(response->GetStatus()),                                  \
            response->GetErrorReason());                                       \
        return response;                                                       \
    }                                                                          \
// BLOCKSTORE_DECLARE_METHOD

    BLOCKSTORE_DECLARE_METHOD(ReadBlocks, TEvService);
    BLOCKSTORE_DECLARE_METHOD(WriteBlocks, TEvService);
    BLOCKSTORE_DECLARE_METHOD(ReadBlocksLocal, TEvService);
    BLOCKSTORE_DECLARE_METHOD(WriteBlocksLocal, TEvService);
    BLOCKSTORE_DECLARE_METHOD(ZeroBlocks, TEvService);
    BLOCKSTORE_DECLARE_METHOD(CheckRange, TEvVolume);
    BLOCKSTORE_DECLARE_METHOD(ChecksumBlocks, TEvNonreplPartitionPrivate);

#undef BLOCKSTORE_DECLARE_METHOD
};

inline void ToLogicalBlocks(NProto::TDeviceConfig& device, ui32 blockSize)
{
    const auto k = blockSize / DefaultDeviceBlockSize;

    device.SetBlocksCount(device.GetBlocksCount() / k);
    device.SetBlockSize(blockSize);
}

inline TDevices ToLogicalBlocks(TDevices devices, ui32 blockSize)
{
    for (auto& device: devices) {
        ToLogicalBlocks(device, blockSize);
    }

    return devices;
}

////////////////////////////////////////////////////////////////////////////////

inline void WaitForMigrations(
    NActors::TTestBasicRuntime& runtime,
    ui32 rangeCount)
{
    ui32 migratedRanges = 0;
    auto old = runtime.SetObserverFunc([&] (auto& event) {
        switch (event->GetTypeRewrite()) {
            case TEvNonreplPartitionPrivate::EvRangeMigrated: {
                auto* msg =
                    event->template Get<TEvNonreplPartitionPrivate::TEvRangeMigrated>();
                if (!HasError(msg->Error)) {
                    ++migratedRanges;
                }
                break;
            }
        }
        return NActors::TTestActorRuntime::DefaultObserverFunc(event);
    });

    ui32 i = 0;
    while (migratedRanges < rangeCount && i++ < 6) {
        NActors::TDispatchOptions options;
        options.FinalEvents = {
            NActors::TDispatchOptions::TFinalEventCondition(
                TEvNonreplPartitionPrivate::EvRangeMigrated)
        };

        runtime.DispatchEvents(options);
    }

    UNIT_ASSERT_VALUES_EQUAL(rangeCount, migratedRanges);
    runtime.SetObserverFunc(std::move(old));
}

inline void WaitForNoMigrations(NActors::TTestBasicRuntime& runtime, TDuration timeout)
{
    ui32 migratedRanges = 0;
    auto old = runtime.SetObserverFunc([&] (auto& event) {
        switch (event->GetTypeRewrite()) {
            case TEvNonreplPartitionPrivate::EvRangeMigrated: {
                auto* msg =
                    event->template Get<TEvNonreplPartitionPrivate::TEvRangeMigrated>();
                if (!HasError(msg->Error)) {
                    ++migratedRanges;
                }
                break;
            }
        }
        return NActors::TTestActorRuntime::DefaultObserverFunc(event);
    });

    NActors::TDispatchOptions options;
    options.FinalEvents = {
        NActors::TDispatchOptions::TFinalEventCondition(
            TEvNonreplPartitionPrivate::EvRangeMigrated)
    };

    runtime.DispatchEvents(options, timeout);

    UNIT_ASSERT_VALUES_EQUAL(0, migratedRanges);
    runtime.SetObserverFunc(std::move(old));
}

}   // namespace NCloud::NBlockStore::NStorage
