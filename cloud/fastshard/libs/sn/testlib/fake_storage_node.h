#pragma once

#include <cloud/fastshard/libs/sn/iface/storage_node.h>
#include <cloud/fastshard/public/api/protos/device.pb.h>

#include <util/generic/deque.h>
#include <util/generic/vector.h>
#include <util/system/spinlock.h>

namespace NCloud::NFastShard {

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

    TVector<NProto::TAcquireDevicesRequest> AcquireCalls;
    TVector<NProto::TReleaseDevicesRequest> ReleaseCalls;
    TVector<NProto::TFormatDeviceRequest> FormatCalls;
    TVector<NProto::TListDevicesRequest> ListCalls;
    TVector<NProto::TReadPagesRequest> ReadCalls;
    TVector<NProto::TWriteLogRecordRequest> WriteCalls;
    TVector<NProto::TReadJournalTailRequest> ReadJournalTailCalls;
    TVector<NProto::TAdvanceLsnLowWatermarkRequest> AdvanceLsnLowWatermarkCalls;
    TVector<NProto::TWritePagesRequest> WritePagesCalls;

    NProto::TAcquireDevicesResponse AcquireResp;
    NProto::TReleaseDevicesResponse ReleaseResp;
    NProto::TFormatDeviceResponse FormatResp;
    NProto::TListDevicesResponse ListResp;
    NProto::TReadPagesResponse ReadResp;
    NProto::TWriteLogRecordResponse WriteResp;
    NProto::TReadJournalTailResponse ReadJournalTailResp;
    NProto::TAdvanceLsnLowWatermarkResponse AdvanceLsnLowWatermarkResp;
    NProto::TWritePagesResponse WritePagesResp;

    TDeque<NProto::TAcquireDevicesResponse> AcquireRespQueue;
    TDeque<NProto::TReleaseDevicesResponse> ReleaseRespQueue;
    TDeque<NProto::TFormatDeviceResponse> FormatRespQueue;
    TDeque<NProto::TListDevicesResponse> ListRespQueue;
    TDeque<NProto::TReadPagesResponse> ReadRespQueue;
    TDeque<NProto::TWriteLogRecordResponse> WriteRespQueue;
    TDeque<NProto::TReadJournalTailResponse> ReadJournalTailRespQueue;
    TDeque<NProto::TWritePagesResponse> WritePagesRespQueue;

    NProto::TAcquireDevicesResponse AcquireDevices(
        TFastShardContext&,
        NProto::TAcquireDevicesRequest request) override;

    NProto::TReleaseDevicesResponse ReleaseDevices(
        TFastShardContext&,
        NProto::TReleaseDevicesRequest request) override;

    NProto::TFormatDeviceResponse FormatDevice(
        TFastShardContext&,
        NProto::TFormatDeviceRequest request) override;

    NProto::TListDevicesResponse ListDevices(
        TFastShardContext&,
        NProto::TListDevicesRequest request) override;

    NProto::TReadPagesResponse ReadPages(
        TFastShardContext&,
        NProto::TReadPagesRequest request) override;

    NProto::TWriteLogRecordResponse WriteLogRecord(
        TFastShardContext&,
        NProto::TWriteLogRecordRequest request) override;

    NProto::TReadJournalTailResponse ReadJournalTail(
        TFastShardContext&,
        NProto::TReadJournalTailRequest request) override;

    NProto::TAdvanceLsnLowWatermarkResponse AdvanceLsnLowWatermark(
        TFastShardContext&,
        NProto::TAdvanceLsnLowWatermarkRequest request) override;

    NProto::TWritePagesResponse WritePages(
        TFastShardContext&,
        NProto::TWritePagesRequest request) override;
};

}   // namespace NCloud::NFastShard
