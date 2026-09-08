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

class TStorageGroupImpl final: public IStorageGroup
{
private:
    const TStorageGroupConfig Config;
    TVector<TStorageDevice> Devices;
    ITimerPtr Timer;
    std::atomic<ui32> Selector{0};
    std::atomic<ui64> LastLsn{0};
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
    // The naive group does no recovery: Init is just the acquire.
    NProto::TError Init() override
    {
        NProto::TAcquireDevicesRequest request;
        request.SetGeneration(Config.AcquireGeneration);
        return MirrorRequest<NProto::TAcquireDevicesResponse>(
            Config,
            Devices,
            *Timer,
            AcquireDevicesFiberMain,
            std::move(request));
    }

    void TearDown() override
    {
        if (TornDown) {
            return;
        }
        TornDown = true;

        auto error = MirrorRequest<NProto::TReleaseDevicesResponse>(
            Config,
            Devices,
            *Timer,
            ReleaseDevicesFiberMain,
            NProto::TReleaseDevicesRequest{});
        if (HasError(error)) {
            SILK_WARN("sg release error=%s", FormatError(error).c_str());
        }
    }

    NProto::TError WriteLogRecord(
        NProto::TDeviceRequestHeaders headers,
        TVector<TPageGroup> pageGroups,
        ui64 lsn) override
    {
        FillHeaders(Config, &headers);
        auto request = MakeWriteLogRecordRequest(
            std::move(headers),
            pageGroups,
            lsn);
        request.SetPrevLogSequenceNumber(LastLsn.exchange(lsn));
        SILK_DEBUG("sg write: %s", DebugMessage(request).c_str());

        return MirrorRequest<NProto::TWriteLogRecordResponse>(
            Config,
            Devices,
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

        FillHeaders(Config, &headers);
        auto request = MakeReadPagesRequest(std::move(headers), pageGroupRefs);
        auto response = CallWithRetries(
            Config.RetryPolicy,
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
