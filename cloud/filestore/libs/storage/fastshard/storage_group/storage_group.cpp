#include "storage_group.h"

#include "context.h"
#include "storage_group_helpers.h"

#include <cloud/storage/core/libs/common/error.h>

#include <silk/fibers/fiber.h>
#include <silk/fibers/future.h>
#include <silk/util/logger.h>

#include <util/generic/vector.h>

#include <atomic>
#include <memory>

namespace NCloud::NFileStore::NStorage::NFastShard {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TFiberTimer final: public ITimer
{
public:
    TInstant Now() override
    {
        return TInstant::Now();
    }

    void Sleep(TDuration duration) override
    {
        silk::FiberScheduler::sleep(duration.NanoSeconds());
    }
};

////////////////////////////////////////////////////////////////////////////////

/**
 * Sends @p request to every device and waits for all of them - an n/n fan-out
 * with no early return. Returns the first error observed, or an empty error if
 * every device acked. The per-device responses go to @p responses if given.
 *
 * Everything the spawned fibers touch lives on this frame, which is safe
 * precisely because the call joins all of them before returning. A fan-out that
 * returns early cannot be written this way.
 */
template <typename TResponse, typename TRequest, typename TParams>
NProto::TError MirrorRequest(
    const TStorageGroupConfig& config,
    const TVector<TStorageDevice>& devices,
    TFastShardContext& ctx,
    int (*fiberMain)(TParams*) noexcept,
    TRequest request,
    TVector<TResponse>* responses = nullptr)
{
    FillHeaders(config, request.MutableHeaders());

    const ui32 count = devices.size();
    TVector<silk::FiberFuture> futures(count);
    TVector<TResponse> ownResponses;
    if (!responses) {
        responses = &ownResponses;
    }
    responses->assign(count, {});

    for (ui32 i = 0; i < count; ++i) {
        const int r = silk::FiberScheduler::run(
            fiberMain,
            TParams{
                .Device = devices[i],
                .Request = &request,
                .Response = &(*responses)[i],
                .RetryPolicy = &config.RetryPolicy,
                .Context = &ctx},
            &futures[i]);
        Y_ABORT_UNLESS(r == 0, "failed to spawn fiber: %s", ::strerror(r));
    }

    NProto::TError error;
    for (ui32 i = 0; i < count; ++i) {
        const int r = futures[i].wait();
        if (r) {
            SILK_ERROR("future error: %s", ::strerror(r));
            if (!HasError(error)) {
                error = MakeError(MAKE_SYSTEM_ERROR(r));
            }
            continue;
        }

        auto& response = (*responses)[i];
        if (HasError(response.GetError())) {
            SILK_ERROR(
                "node error: %s",
                FormatError(response.GetError()).c_str());
            if (!HasError(error)) {
                error = response.GetError();
            }
        }
    }

    return error;
}

////////////////////////////////////////////////////////////////////////////////

struct TAcquireDevicesParams
{
    TStorageDevice Device;
    NProto::TAcquireDevicesRequest* Request;
    NProto::TAcquireDevicesResponse* Response;
    const TStorageGroupRetryPolicy* RetryPolicy;
    TFastShardContext* Context;
};

int AcquireDevicesFiberMain(TAcquireDevicesParams* params) noexcept
{
    NProto::TAcquireDevicesRequest request = *params->Request;
    request.AddDeviceUUIDs(params->Device.DeviceUUID);
    *params->Response = CallWithRetries(
        *params->Context,
        *params->RetryPolicy,
        [&] { return params->Device.Node->AcquireDevices(request); });

    return 0;
}

struct TReleaseDevicesParams
{
    TStorageDevice Device;
    NProto::TReleaseDevicesRequest* Request;
    NProto::TReleaseDevicesResponse* Response;
    const TStorageGroupRetryPolicy* RetryPolicy;
    TFastShardContext* Context;
};

int ReleaseDevicesFiberMain(TReleaseDevicesParams* params) noexcept
{
    NProto::TReleaseDevicesRequest request = *params->Request;
    request.AddDeviceUUIDs(params->Device.DeviceUUID);
    *params->Response = CallWithRetries(
        *params->Context,
        *params->RetryPolicy,
        [&] { return params->Device.Node->ReleaseDevices(request); });

    return 0;
}

struct TWriteLogRecordParams
{
    TStorageDevice Device;
    NProto::TWriteLogRecordRequest* Request;
    NProto::TWriteLogRecordResponse* Response;
    const TStorageGroupRetryPolicy* RetryPolicy;
    TFastShardContext* Context;
};

int WriteLogRecordFiberMain(TWriteLogRecordParams* params) noexcept
{
    NProto::TWriteLogRecordRequest request = *params->Request;
    request.SetDeviceUUID(std::move(params->Device.DeviceUUID));
    *params->Response = CallWithRetries(
        *params->Context,
        *params->RetryPolicy,
        [&] { return params->Device.Node->WriteLogRecord(request); });
    return 0;
}

struct TReadJournalTailParams
{
    TStorageDevice Device;
    NProto::TReadJournalTailRequest* Request;
    NProto::TReadJournalTailResponse* Response;
    const TStorageGroupRetryPolicy* RetryPolicy;
    TFastShardContext* Context;
};

int ReadJournalTailFiberMain(TReadJournalTailParams* params) noexcept
{
    NProto::TReadJournalTailRequest request = *params->Request;
    request.SetDeviceUUID(params->Device.DeviceUUID);
    *params->Response = CallWithRetries(
        *params->Context,
        *params->RetryPolicy,
        [&] { return params->Device.Node->ReadJournalTail(request); });

    return 0;
}

////////////////////////////////////////////////////////////////////////////////

class TStorageGroupImpl final: public IStorageGroup
{
private:
    const TStorageGroupConfig Config;
    TVector<TStorageDevice> Devices;
    ITimerPtr Timer;
    std::atomic<bool> Cancelled = false;
    std::atomic<ui64> NextRequestId = 1;
    std::atomic<ui32> Selector{0};
    bool TornDown = false;

public:
    TStorageGroupImpl(
            TStorageGroupConfig config,
            TVector<TStorageDevice> devices,
            ITimerPtr timer)
        : Config(std::move(config))
        , Devices(std::move(devices))
        , Timer(std::move(timer))
    {}

public:
    // The naive group does no recovery: Init is the acquire plus a look at
    // where every device's journal ends.
    TResultOrError<ui64> Init() override
    {
        auto ctx = MakeContext();
        NProto::TAcquireDevicesRequest acquire;
        acquire.SetGeneration(Config.AcquireGeneration);
        auto error = MirrorRequest<NProto::TAcquireDevicesResponse>(
            Config,
            Devices,
            ctx,
            AcquireDevicesFiberMain,
            std::move(acquire));
        if (HasError(error)) {
            return error;
        }

        NProto::TReadJournalTailRequest tail;
        tail.SetMaxRecordCount(1);
        TVector<NProto::TReadJournalTailResponse> responses;
        error = MirrorRequest<NProto::TReadJournalTailResponse>(
            Config,
            Devices,
            ctx,
            ReadJournalTailFiberMain,
            std::move(tail),
            &responses);
        if (HasError(error)) {
            return error;
        }

        ui64 lastLsn = 0;
        for (const auto& response: responses) {
            lastLsn = Max(lastLsn, response.GetLsnLowWatermark());
        }

        return lastLsn;
    }

    void TearDown() override
    {
        if (TornDown) {
            return;
        }
        TornDown = true;

        auto ctx = MakeContext();
        auto error = MirrorRequest<NProto::TReleaseDevicesResponse>(
            Config,
            Devices,
            ctx,
            ReleaseDevicesFiberMain,
            NProto::TReleaseDevicesRequest{});
        if (HasError(error)) {
            SILK_WARN("sg release error=%s", FormatError(error).c_str());
        }
    }

    NProto::TError WriteLogRecord(
        NProto::TDeviceRequestHeaders headers,
        TVector<TPageGroup> pageGroups,
        TLsnLink link) override
    {
        FillHeaders(Config, &headers);
        auto request = MakeWriteLogRecordRequest(
            std::move(headers),
            pageGroups,
            link);
        SILK_DEBUG("sg write: %s", DebugMessage(request).c_str());

        auto ctx = MakeContext();
        return MirrorRequest<NProto::TWriteLogRecordResponse>(
            Config,
            Devices,
            ctx,
            WriteLogRecordFiberMain,
            std::move(request));
    }

    NProto::TError ReadPages(
        NProto::TDeviceRequestHeaders headers,
        const TVector<TPageGroupRef>& pageGroupRefs,
        TVector<TPageGroup>* pageGroups) override
    {
        pageGroups->clear();

        FillHeaders(Config, &headers);
        auto request = MakeReadPagesRequest(
            std::move(headers),
            pageGroupRefs,
            Config.PageSize);
        auto ctx = MakeContext();
        auto response = CallWithRetries(
            ctx,
            Config.RetryPolicy,
            [&]
            {
                const ui32 i =
                    Selector.fetch_add(1, std::memory_order_relaxed) %
                    Devices.size();
                request.SetDeviceUUID(Devices[i].DeviceUUID);
                SILK_DEBUG(
                    "sg read: %s",
                    request.ShortUtf8DebugString().c_str());
                return Devices[i].Node->ReadPages(request);
            });

        if (!HasError(response.GetError())) {
            ExtractPageGroups(response, pageGroups);
        }

        return response.GetError();
    }

private:
    TFastShardContext MakeContext()
    {
        return {
            NextRequestId++,
            *Timer,
            Cancelled,
            Config.RetryPolicy.TotalTimeout};
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IStorageGroupPtr CreateNaiveMirroredStorageGroup(
    TStorageGroupConfig config,
    TVector<TStorageDevice> devices,
    ITimerPtr timer)
{
    return std::make_shared<TStorageGroupImpl>(
        std::move(config),
        std::move(devices),
        std::move(timer));
}

ITimerPtr CreateFiberTimer()
{
    return std::make_shared<TFiberTimer>();
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
