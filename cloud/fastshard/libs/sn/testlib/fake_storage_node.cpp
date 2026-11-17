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

NProto::TAcquireDevicesResponse TFakeStorageNode::AcquireDevices(
    TFastShardContext&,
    NProto::TAcquireDevicesRequest request)
{
    with_lock (Lock) {
        AcquireCalls.push_back(std::move(request));
        return PopResponse(AcquireRespQueue, AcquireResp);
    }
}

NProto::TReleaseDevicesResponse TFakeStorageNode::ReleaseDevices(
    TFastShardContext&,
    NProto::TReleaseDevicesRequest request)
{
    with_lock (Lock) {
        ReleaseCalls.push_back(std::move(request));
        return PopResponse(ReleaseRespQueue, ReleaseResp);
    }
}

NProto::TFormatDeviceResponse TFakeStorageNode::FormatDevice(
    TFastShardContext&,
    NProto::TFormatDeviceRequest request)
{
    with_lock (Lock) {
        FormatCalls.push_back(std::move(request));
        return PopResponse(FormatRespQueue, FormatResp);
    }
}

NProto::TListDevicesResponse TFakeStorageNode::ListDevices(
    TFastShardContext&,
    NProto::TListDevicesRequest request)
{
    with_lock (Lock) {
        ListCalls.push_back(std::move(request));
        return PopResponse(ListRespQueue, ListResp);
    }
}

NProto::TReadPagesResponse TFakeStorageNode::ReadPages(
    TFastShardContext&,
    NProto::TReadPagesRequest request)
{
    with_lock (Lock) {
        ReadCalls.push_back(std::move(request));
        return PopResponse(ReadRespQueue, ReadResp);
    }
}

NProto::TWriteLogRecordResponse TFakeStorageNode::WriteLogRecord(
    TFastShardContext&,
    NProto::TWriteLogRecordRequest request)
{
    with_lock (Lock) {
        WriteCalls.push_back(std::move(request));
        return PopResponse(WriteRespQueue, WriteResp);
    }
}

NProto::TReadJournalTailResponse TFakeStorageNode::ReadJournalTail(
    TFastShardContext&,
    NProto::TReadJournalTailRequest request)
{
    with_lock (Lock) {
        ReadJournalTailCalls.push_back(std::move(request));
        return PopResponse(ReadJournalTailRespQueue, ReadJournalTailResp);
    }
}

NProto::TAdvanceLsnLowWatermarkResponse
TFakeStorageNode::AdvanceLsnLowWatermark(
    TFastShardContext&,
    NProto::TAdvanceLsnLowWatermarkRequest request)
{
    with_lock (Lock) {
        AdvanceLsnLowWatermarkCalls.push_back(std::move(request));
    }
    return AdvanceLsnLowWatermarkResp;
}

NProto::TWritePagesResponse TFakeStorageNode::WritePages(
    TFastShardContext&,
    NProto::TWritePagesRequest request)
{
    with_lock (Lock) {
        WritePagesCalls.push_back(std::move(request));
        return PopResponse(WritePagesRespQueue, WritePagesResp);
    }
}

}   // namespace NCloud::NFastShard
