#include "storage_group.h"
#include "storage_group_helpers.h"

#include <silk/fibers/fiber.h>
#include <silk/fibers/future.h>
#include <silk/util/logger.h>

#include <util/generic/vector.h>

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

struct TWriteLogRecordParams
{
    TStorageDevice Device;
    NProto::TWriteLogRecordRequest* Request;
    NProto::TWriteLogRecordResponse* Response;
    const TStorageGroupRetryPolicy* RetryPolicy;
    ITimer* Timer;
};

int WriteLogRecordFiberMain(TWriteLogRecordParams* params) noexcept
{
    NProto::TWriteLogRecordRequest request = *params->Request;
    request.SetDeviceUUID(std::move(params->Device.DeviceUUID));
    *params->Response = CallWithRetries(
        *params->RetryPolicy,
        *params->Timer,
        [&] { return params->Device.Node->WriteLogRecord(request); });
    return 0;
}

////////////////////////////////////////////////////////////////////////////////

class TStorageGroupImpl: public IStorageGroup
{
private:
    TVector<TStorageDevice> Devices;
    const TStorageGroupRetryPolicy RetryPolicy;
    ITimerPtr Timer;
    std::atomic<ui32> Selector{0};
    std::atomic<ui64> LastLsn{0};

public:
    TStorageGroupImpl(
            TVector<TStorageDevice> devices,
            TStorageGroupRetryPolicy retryPolicy,
            ITimerPtr timer)
        : Devices(std::move(devices))
        , RetryPolicy(retryPolicy)
        , Timer(std::move(timer))
    {}

public:
    NProto::TError AcquireDevices() override
    {
        return MirrorRequest<NProto::TAcquireDevicesResponse>(
            Devices,
            RetryPolicy,
            *Timer,
            AcquireDevicesFiberMain,
            NProto::TAcquireDevicesRequest{});
    }

    NProto::TError ReleaseDevices() override
    {
        return MirrorRequest<NProto::TReleaseDevicesResponse>(
            Devices,
            RetryPolicy,
            *Timer,
            ReleaseDevicesFiberMain,
            NProto::TReleaseDevicesRequest{});
    }

    NProto::TError WriteLogRecord(
        NProto::TDeviceRequestHeaders headers,
        TVector<TPageGroup> pageGroups,
        ui64 lsn) override
    {
        auto request = MakeWriteLogRecordRequest(
            std::move(headers),
            pageGroups,
            lsn);

        request.SetPrevLogSequenceNumber(LastLsn.exchange(lsn));
        SILK_DEBUG("sg write: %s", DebugMessage(request).c_str());

        return MirrorRequest<NProto::TWriteLogRecordResponse>(
            Devices,
            RetryPolicy,
            *Timer,
            WriteLogRecordFiberMain,
            std::move(request));
    }

    NProto::TError ReadPages(
        NProto::TDeviceRequestHeaders headers,
        const TVector<TPageGroupRef>& pageGroupRefs,
        TVector<TPageGroup>* pageGroups) override
    {
        pageGroups->clear();

        auto request = MakeReadPagesRequest(std::move(headers), pageGroupRefs);
        auto response = CallWithRetries(
            RetryPolicy,
            *Timer,
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
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IStorageGroupPtr CreateNaiveMirroredStorageGroup(
    TVector<TStorageDevice> devices,
    TStorageGroupRetryPolicy retryPolicy,
    ITimerPtr timer)
{
    return std::make_shared<TStorageGroupImpl>(
        std::move(devices),
        retryPolicy,
        std::move(timer));
}

ITimerPtr CreateFiberTimer()
{
    return std::make_shared<TFiberTimer>();
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
