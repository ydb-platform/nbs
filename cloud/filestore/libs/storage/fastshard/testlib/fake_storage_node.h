#pragma once

#include <cloud/filestore/libs/storage/fastshard/sn/iface/storage_node.h>

#include <cloud/storage/core/protos/device.pb.h>

#include <util/generic/deque.h>
#include <util/generic/vector.h>
#include <util/system/spinlock.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

/**
 * IStorageNode used from tests. Records every incoming request in a
 * TAdaptiveLock-guarded TVector per method, and returns the
 * corresponding preconfigured response (also per method).
 *
 * Typical use: construct, edit the *Resp members to set up canned
 * output, register the fake with an sn server, drive it through the
 * client, then inspect the *Calls vectors to check what the server
 * dispatched.
 *
 * The *RespQueue members hold one-shot responses: while a queue is not
 * empty, each call pops and returns its front element; after the queue
 * drains, the corresponding canned *Resp member is returned. Useful for
 * scripting transient errors in retry tests.
 */
struct TFakeStorageNode: public IStorageNode
{
    TAdaptiveLock Lock;

    TVector<NCloud::NProto::TAcquireDevicesRequest> AcquireCalls;
    TVector<NCloud::NProto::TReleaseDevicesRequest> ReleaseCalls;
    TVector<NCloud::NProto::TReadPagesRequest> ReadCalls;
    TVector<NCloud::NProto::TWriteLogRecordRequest> WriteCalls;
    TVector<NCloud::NProto::TReadJournalTailRequest> ReadJournalTailCalls;
    TVector<NCloud::NProto::TAdvanceLsnLowWatermarkRequest>
        AdvanceLsnLowWatermarkCalls;

    NCloud::NProto::TAcquireDevicesResponse AcquireResp;
    NCloud::NProto::TReleaseDevicesResponse ReleaseResp;
    NCloud::NProto::TReadPagesResponse ReadResp;
    NCloud::NProto::TWriteLogRecordResponse WriteResp;
    NCloud::NProto::TReadJournalTailResponse ReadJournalTailResp;
    NCloud::NProto::TAdvanceLsnLowWatermarkResponse
        AdvanceLsnLowWatermarkResp;

    TDeque<NCloud::NProto::TAcquireDevicesResponse> AcquireRespQueue;
    TDeque<NCloud::NProto::TReleaseDevicesResponse> ReleaseRespQueue;
    TDeque<NCloud::NProto::TReadPagesResponse> ReadRespQueue;
    TDeque<NCloud::NProto::TWriteLogRecordResponse> WriteRespQueue;

    NCloud::NProto::TAcquireDevicesResponse AcquireDevices(
        NCloud::NProto::TAcquireDevicesRequest request) override;

    NCloud::NProto::TReleaseDevicesResponse ReleaseDevices(
        NCloud::NProto::TReleaseDevicesRequest request) override;

    NCloud::NProto::TReadPagesResponse ReadPages(
        NCloud::NProto::TReadPagesRequest request) override;

    NCloud::NProto::TWriteLogRecordResponse WriteLogRecord(
        NCloud::NProto::TWriteLogRecordRequest request) override;

    NCloud::NProto::TReadJournalTailResponse ReadJournalTail(
        NCloud::NProto::TReadJournalTailRequest request) override;

    NCloud::NProto::TAdvanceLsnLowWatermarkResponse AdvanceLsnLowWatermark(
        NCloud::NProto::TAdvanceLsnLowWatermarkRequest request) override;
};

}   // namespace NCloud::NFileStore::NStorage::NFastShard
