#include "journalled_storage_node.h"

#include <cloud/fastshard/libs/journal/iface/journalled_device.h>
#include <cloud/fastshard/libs/journal/impl/device_page_store.h>
#include <cloud/fastshard/libs/journal/impl/journal.h>
#include <cloud/fastshard/libs/journal/impl/journalled_device_v2.h>
#include <cloud/fastshard/libs/journal/impl/key_buffer_store.h>
#include <cloud/fastshard/libs/journal/impl/memory_device.h>
#include <cloud/fastshard/libs/journal/server/proto_helpers.h>

#include <cloud/storage/core/libs/common/error.h>

#include <silk/fibers/future.h>

#include <library/cpp/threading/future/future.h>

#include <util/string/builder.h>

namespace NCloud::NFastShard {

using namespace NJournalled;

namespace {

////////////////////////////////////////////////////////////////////////////////

// Parks the calling fiber until the future is set on the device's executor.
template <typename T>
T WaitInFiber(NThreading::TFuture<T> future)
{
    auto ready = std::make_shared<silk::FiberFuture>();
    future.Subscribe([ready] (const auto&) { ready->set(0); });
    ready->wait();
    return future.ExtractValue();
}

// The device does not know its UUID, so the node checks the address of a
// request the way the journal service does.
NProto::TError CheckDeviceUUID(
    const TString& requestDeviceUUID,
    const TString& deviceUUID)
{
    if (requestDeviceUUID.empty()) {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder() << "empty device UUID in the request to device "
                             << deviceUUID.Quote());
    }

    if (requestDeviceUUID != deviceUUID) {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder() << "the request is addressed to device "
                             << requestDeviceUUID.Quote() << ", this is "
                             << deviceUUID.Quote());
    }

    return {};
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TJournalledStorageNode::TJournalledStorageNode(
        TString deviceUUID,
        ILoggingServicePtr logging,
        TExecutorPtr executor,
        TJournalledDeviceLayout layout)
    : DeviceUUID(std::move(deviceUUID))
    , Layout(layout)
    , Logging(std::move(logging))
    , Executor(std::move(executor))
    , LogMetaDevice(CreateInMemoryDevice(Layout.PageSize))
    , LogDataDevice(CreateInMemoryDevice(Layout.PageSize))
    , DataDevice(CreateInMemoryDevice(Layout.PageSize))
{
    auto journal = CreateJournal(
        Logging,
        Executor,
        CreateDeviceKeyBufferStore(
            Logging,
            LogMetaDevice,
            Layout.LogMetaPageCount,
            Layout.PageSize,
            CalculateMinRecordBufferSize()),
        CreateDevicePageStore(
            LogDataDevice,
            Layout.LogDataPageCount,
            Layout.PageSize,
            EDevicePageStoreMode::Checked),
        DeviceUUID,
        Layout.DataPageCount,
        Layout.PageSize);

    Device = CreateJournalledDeviceV2(
        Logging,
        Executor,
        std::move(journal),
        DataDevice,
        DeviceUUID);
}

void TJournalledStorageNode::Start()
{
    Y_ABORT_UNLESS(!Started, "%s is started", DeviceUUID.c_str());

    Device->Start().Wait();
    Started = true;
}

void TJournalledStorageNode::Stop()
{
    if (!Started) {
        return;
    }

    Device->Stop().Wait();
    Started = false;
}

NProto::TAcquireDevicesResponse TJournalledStorageNode::AcquireDevices(
    TFastShardContext&,
    NProto::TAcquireDevicesRequest)
{
    ++Counters.AcquireDevices;
    return {};
}

NProto::TReleaseDevicesResponse TJournalledStorageNode::ReleaseDevices(
    TFastShardContext&,
    NProto::TReleaseDevicesRequest)
{
    ++Counters.ReleaseDevices;
    return {};
}

NProto::TFormatDeviceResponse TJournalledStorageNode::FormatDevice(
    TFastShardContext&,
    NProto::TFormatDeviceRequest)
{
    ++Counters.FormatDevice;
    return {};
}

NProto::TListDevicesResponse TJournalledStorageNode::ListDevices(
    TFastShardContext&,
    NProto::TListDevicesRequest)
{
    ++Counters.ListDevices;

    NProto::TListDevicesResponse response;
    auto& info = *response.AddDevices();
    info.SetDeviceUUID(DeviceUUID);
    info.SetBlockSize(Layout.PageSize);
    info.SetLogMetaSize(Layout.LogMetaPageCount * Layout.PageSize);
    info.SetLogDataSize(Layout.LogDataPageCount * Layout.PageSize);
    info.SetDataSize(Layout.DataPageCount * Layout.PageSize);
    return response;
}

NProto::TReadPagesResponse TJournalledStorageNode::ReadPages(
    TFastShardContext&,
    NProto::TReadPagesRequest request)
{
    ++Counters.ReadPages;
    if (auto error = CheckDeviceUUID(request.GetDeviceUUID(), DeviceUUID);
        HasError(error))
    {
        return TErrorResponse(std::move(error));
    }

    if (!Started) {
        return TErrorResponse(E_INVALID_STATE, "not started");
    }

    const ui64 pageCount = GetPageCount(request);
    if (pageCount > Layout.DataPageCount) {
        return TErrorResponse(
            E_ARGUMENT,
            TStringBuilder() << "the request reads " << pageCount
                             << " pages, the device has "
                             << Layout.DataPageCount);
    }

    // the device reads the pages straight into the response
    NProto::TReadPagesResponse response;

    auto result = WaitInFiber(Device->ReadPages(
        MakePageRangeViews(request, Layout.PageSize, &response)));
    if (HasError(result)) {
        return TErrorResponse(result.GetError());
    }

    response.SetLastAckedLogSequenceNumber(result.GetResult());
    return response;
}

NProto::TWriteLogRecordResponse TJournalledStorageNode::WriteLogRecord(
    TFastShardContext&,
    NProto::TWriteLogRecordRequest request)
{
    ++Counters.WriteLogRecord;
    if (auto error = CheckDeviceUUID(request.GetDeviceUUID(), DeviceUUID);
        HasError(error))
    {
        return TErrorResponse(std::move(error));
    }

    if (!Started) {
        return TErrorResponse(E_INVALID_STATE, "not started");
    }

    return TErrorResponse(WaitInFiber(Device->WriteLogRecord(
        MakePageRanges(request),
        request.GetLogSequenceNumber(),
        request.GetPrevLogSequenceNumber())));
}

NProto::TReadJournalTailResponse TJournalledStorageNode::ReadJournalTail(
    TFastShardContext&,
    NProto::TReadJournalTailRequest request)
{
    ++Counters.ReadJournalTail;
    if (auto error = CheckDeviceUUID(request.GetDeviceUUID(), DeviceUUID);
        HasError(error))
    {
        return TErrorResponse(std::move(error));
    }

    if (!Started) {
        return TErrorResponse(E_INVALID_STATE, "not started");
    }

    auto result = WaitInFiber(Device->ReadJournalTail(
        request.GetAfterLogSequenceNumber(),
        request.GetMaxRecordCount()));
    if (HasError(result)) {
        return TErrorResponse(result.GetError());
    }

    return MakeReadJournalTailResponse(result.GetResult());
}

NProto::TAdvanceLsnLowWatermarkResponse
TJournalledStorageNode::AdvanceLsnLowWatermark(
    TFastShardContext&,
    NProto::TAdvanceLsnLowWatermarkRequest request)
{
    ++Counters.AdvanceLsnLowWatermark;
    if (auto error = CheckDeviceUUID(request.GetDeviceUUID(), DeviceUUID);
        HasError(error))
    {
        return TErrorResponse(std::move(error));
    }

    if (!Started) {
        return TErrorResponse(E_INVALID_STATE, "not started");
    }

    return TErrorResponse(
        WaitInFiber(Device->AdvanceWatermark(request.GetLsnLowWatermark())));
}

NProto::TWritePagesResponse TJournalledStorageNode::WritePages(
    TFastShardContext&,
    NProto::TWritePagesRequest request)
{
    ++Counters.WritePages;
    if (auto error = CheckDeviceUUID(request.GetDeviceUUID(), DeviceUUID);
        HasError(error))
    {
        return TErrorResponse(std::move(error));
    }

    if (!Started) {
        return TErrorResponse(E_INVALID_STATE, "not started");
    }

    return TErrorResponse(
        WaitInFiber(Device->WritePages(MakePageRanges(request))));
}

}   // namespace NCloud::NFastShard
