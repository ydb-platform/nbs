#include "journalled_storage_node.h"

#include <cloud/fastshard/journal/iface/journalled_device.h>
#include <cloud/fastshard/journal/impl/device_page_store.h>
#include <cloud/fastshard/journal/impl/journal.h>
#include <cloud/fastshard/journal/impl/journalled_device_v2.h>
#include <cloud/fastshard/journal/impl/key_buffer_store.h>
#include <cloud/fastshard/journal/impl/memory_device.h>
#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/threading/future/future.h>

#include <silk/fibers/future.h>

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
            Layout.PageSize),
        CreateDevicePageStore(
            LogDataDevice,
            Layout.LogDataPageCount,
            Layout.PageSize),
        Layout.DataPageCount);

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
    NProto::TAcquireDevicesRequest)
{
    ++Counters.AcquireDevices;
    return {};
}

NProto::TReleaseDevicesResponse TJournalledStorageNode::ReleaseDevices(
    NProto::TReleaseDevicesRequest)
{
    ++Counters.ReleaseDevices;
    return {};
}

NProto::TFormatDeviceResponse TJournalledStorageNode::FormatDevice(
    NProto::TFormatDeviceRequest)
{
    ++Counters.FormatDevice;
    return {};
}

#define SN_FORWARD(name)                                                       \
    NProto::T##name##Response TJournalledStorageNode::name(                    \
        NProto::T##name##Request request)                                      \
    {                                                                          \
        ++Counters.name;                                                       \
        if (!Started) {                                                        \
            return TErrorResponse(E_INVALID_STATE, "not started");             \
        }                                                                      \
        return WaitInFiber(Device->name(std::move(request)));                  \
    }                                                                          \
    // SN_FORWARD

SN_FORWARD(ReadPages)
SN_FORWARD(WriteLogRecord)
SN_FORWARD(ReadJournalTail)
SN_FORWARD(AdvanceLsnLowWatermark)

#undef SN_FORWARD

}   // namespace NCloud::NFastShard
