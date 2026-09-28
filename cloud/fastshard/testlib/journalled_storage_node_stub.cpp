#include "journalled_storage_node.h"

#include <cloud/fastshard/journal/iface/device.h>
#include <cloud/fastshard/journal/iface/journalled_device.h>
#include <cloud/storage/core/libs/common/error.h>

namespace NCloud::NFastShard {

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
    , LogMetaDevice(NJournalled::CreateDeviceStub())
    , LogDataDevice(NJournalled::CreateDeviceStub())
    , DataDevice(NJournalled::CreateDeviceStub())
    , Device(NJournalled::CreateJournalledDeviceStub())
{}

void TJournalledStorageNode::Start()
{}

void TJournalledStorageNode::Stop()
{}

#define SN_STUB(name, ...)                                                     \
    NProto::T##name##Response TJournalledStorageNode::name(                    \
        NProto::T##name##Request)                                              \
    {                                                                          \
        NProto::T##name##Response response;                                    \
        *response.MutableError() = MakeError(E_NOT_IMPLEMENTED);               \
        return response;                                                       \
    }                                                                          \
    // SN_STUB

SN_METHODS(SN_STUB)

#undef SN_STUB

}   // namespace NCloud::NFastShard
