#include "fake_storage_node.h"

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

NCloud::NProto::TAcquireDevicesResponse TFakeStorageNode::AcquireDevices(
    NCloud::NProto::TAcquireDevicesRequest request)
{
    with_lock (Lock) {
        AcquireCalls.push_back(std::move(request));
    }
    return AcquireResp;
}

NCloud::NProto::TReleaseDevicesResponse TFakeStorageNode::ReleaseDevices(
    NCloud::NProto::TReleaseDevicesRequest request)
{
    with_lock (Lock) {
        ReleaseCalls.push_back(std::move(request));
    }
    return ReleaseResp;
}

NCloud::NProto::TReadPagesResponse TFakeStorageNode::ReadPages(
    NCloud::NProto::TReadPagesRequest request)
{
    with_lock (Lock) {
        ReadCalls.push_back(std::move(request));
    }
    return ReadResp;
}

NCloud::NProto::TWriteLogRecordResponse TFakeStorageNode::WriteLogRecord(
    NCloud::NProto::TWriteLogRecordRequest request)
{
    with_lock (Lock) {
        WriteCalls.push_back(std::move(request));
    }
    return WriteResp;
}

NCloud::NProto::TReadJournalTailResponse TFakeStorageNode::ReadJournalTail(
    NCloud::NProto::TReadJournalTailRequest request)
{
    with_lock (Lock) {
        ReadJournalTailCalls.push_back(std::move(request));
    }
    return ReadJournalTailResp;
}

NCloud::NProto::TAdvanceLsnLowWatermarkResponse
TFakeStorageNode::AdvanceLsnLowWatermark(
    NCloud::NProto::TAdvanceLsnLowWatermarkRequest request)
{
    with_lock (Lock) {
        AdvanceLsnLowWatermarkCalls.push_back(std::move(request));
    }
    return AdvanceLsnLowWatermarkResp;
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
