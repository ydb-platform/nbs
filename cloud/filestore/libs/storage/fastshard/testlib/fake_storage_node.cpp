#include "fake_storage_node.h"

namespace NCloud::NFileStore::NStorage::NFastShard {

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename TResponse>
TResponse PopResponse(TDeque<TResponse>& queue, const TResponse& canned)
{
    if (queue.empty()) {
        return canned;
    }

    TResponse response = std::move(queue.front());
    queue.pop_front();
    return response;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

NCloud::NProto::TAcquireDevicesResponse TFakeStorageNode::AcquireDevices(
    NCloud::NProto::TAcquireDevicesRequest request)
{
    with_lock (Lock) {
        AcquireCalls.push_back(std::move(request));
        return PopResponse(AcquireRespQueue, AcquireResp);
    }
}

NCloud::NProto::TReleaseDevicesResponse TFakeStorageNode::ReleaseDevices(
    NCloud::NProto::TReleaseDevicesRequest request)
{
    with_lock (Lock) {
        ReleaseCalls.push_back(std::move(request));
        return PopResponse(ReleaseRespQueue, ReleaseResp);
    }
}

NCloud::NProto::TReadPagesResponse TFakeStorageNode::ReadPages(
    NCloud::NProto::TReadPagesRequest request)
{
    with_lock (Lock) {
        ReadCalls.push_back(std::move(request));
        return PopResponse(ReadRespQueue, ReadResp);
    }
}

NCloud::NProto::TWriteLogRecordResponse TFakeStorageNode::WriteLogRecord(
    NCloud::NProto::TWriteLogRecordRequest request)
{
    with_lock (Lock) {
        WriteCalls.push_back(std::move(request));
        return PopResponse(WriteRespQueue, WriteResp);
    }
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
