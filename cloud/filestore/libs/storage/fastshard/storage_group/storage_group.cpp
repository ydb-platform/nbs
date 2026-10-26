#include "storage_group.h"
#include "storage_group_helpers.h"

#include <cloud/fastshard/common/context.h>

#include <silk/fibers/fiber.h>
#include <silk/fibers/future.h>
#include <silk/util/logger.h>

#include <util/generic/vector.h>

#include <atomic>

namespace NCloud::NFileStore::NStorage::NFastShard {

using NCloud::NFastShard::TFastShardContext;

namespace {

////////////////////////////////////////////////////////////////////////////////

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
    std::atomic<ui64> NextRequestId = 1;
    std::atomic<ui32> Selector{0};
    bool TornDown = false;

public:
    TStorageGroupImpl(
            TStorageGroupConfig config,
            TVector<TStorageDevice> devices)
        : Config(std::move(config))
        , Devices(std::move(devices))
    {}

public:
    // The naive group does no recovery: Init is the acquire plus a look at
    // where every device's journal ends.
    TResultOrError<ui64> Init() override
    {
        TFastShardContext ctx(NextRequestId++);
        NProto::TAcquireDevicesRequest acquire;
        acquire.SetGeneration(Config.AcquireGeneration);
        auto error = MirrorRequest<NProto::TAcquireDevicesResponse>(
            ctx,
            Config,
            Devices,
            AcquireDevicesFiberMain,
            std::move(acquire));
        if (HasError(error)) {
            return error;
        }

        NProto::TReadJournalTailRequest tail;
        tail.SetMaxRecordCount(1);
        TVector<NProto::TReadJournalTailResponse> responses;
        error = MirrorRequest<NProto::TReadJournalTailResponse>(
            ctx,
            Config,
            Devices,
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

        TFastShardContext ctx(NextRequestId++);
        auto error = MirrorRequest<NProto::TReleaseDevicesResponse>(
            ctx,
            Config,
            Devices,
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

        TFastShardContext ctx(NextRequestId++);
        return MirrorRequest<NProto::TWriteLogRecordResponse>(
            ctx,
            Config,
            Devices,
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
        TFastShardContext ctx(NextRequestId++);
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
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IStorageGroupPtr CreateNaiveMirroredStorageGroup(
    TStorageGroupConfig config,
    TVector<TStorageDevice> devices)
{
    return std::make_shared<TStorageGroupImpl>(
        std::move(config),
        std::move(devices));
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
