#include "fake_storage_node.h"

namespace NCloud::NFastShard {

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
    TFastShardContext&,
    NCloud::NProto::TAcquireDevicesRequest request)
{
    with_lock (Lock) {
        AcquireCalls.push_back(std::move(request));
        return PopResponse(AcquireRespQueue, AcquireResp);
    }
}

NCloud::NProto::TReleaseDevicesResponse TFakeStorageNode::ReleaseDevices(
    TFastShardContext&,
    NCloud::NProto::TReleaseDevicesRequest request)
{
    with_lock (Lock) {
        ReleaseCalls.push_back(std::move(request));
        return PopResponse(ReleaseRespQueue, ReleaseResp);
    }
}

NCloud::NProto::TFormatDeviceResponse TFakeStorageNode::FormatDevice(
    TFastShardContext&,
    NCloud::NProto::TFormatDeviceRequest request)
{
    with_lock (Lock) {
        FormatCalls.push_back(std::move(request));
        return PopResponse(FormatRespQueue, FormatResp);
    }
}

NCloud::NProto::TListDevicesResponse TFakeStorageNode::ListDevices(
    TFastShardContext&,
    NCloud::NProto::TListDevicesRequest request)
{
    with_lock (Lock) {
        ListCalls.push_back(std::move(request));
        return PopResponse(ListRespQueue, ListResp);
    }
}

NCloud::NProto::TReadPagesResponse TFakeStorageNode::ReadPages(
    TFastShardContext&,
    NCloud::NProto::TReadPagesRequest request)
{
    with_lock (Lock) {
        ReadCalls.push_back(std::move(request));
        return PopResponse(ReadRespQueue, ReadResp);
    }
}

NCloud::NProto::TWriteLogRecordResponse TFakeStorageNode::WriteLogRecord(
    TFastShardContext&,
    NCloud::NProto::TWriteLogRecordRequest request)
{
    with_lock (Lock) {
        WriteCalls.push_back(std::move(request));
        return PopResponse(WriteRespQueue, WriteResp);
    }
}

NCloud::NProto::TReadJournalTailResponse TFakeStorageNode::ReadJournalTail(
    TFastShardContext&,
    NCloud::NProto::TReadJournalTailRequest request)
{
    with_lock (Lock) {
        ReadJournalTailCalls.push_back(std::move(request));
        return PopResponse(ReadJournalTailRespQueue, ReadJournalTailResp);
    }
}

NCloud::NProto::TAdvanceLsnLowWatermarkResponse
TFakeStorageNode::AdvanceLsnLowWatermark(
    TFastShardContext&,
    NCloud::NProto::TAdvanceLsnLowWatermarkRequest request)
{
    with_lock (Lock) {
        AdvanceLsnLowWatermarkCalls.push_back(std::move(request));
    }
    return AdvanceLsnLowWatermarkResp;
}

}   // namespace NCloud::NFastShard
