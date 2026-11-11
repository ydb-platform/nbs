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

    TVector<NCloud::NProto::TAcquireDevicesRequest> AcquireCalls;
    TVector<NCloud::NProto::TReleaseDevicesRequest> ReleaseCalls;
    TVector<NCloud::NProto::TFormatDeviceRequest> FormatCalls;
    TVector<NCloud::NProto::TListDevicesRequest> ListCalls;
    TVector<NCloud::NProto::TReadPagesRequest> ReadCalls;
    TVector<NCloud::NProto::TWriteLogRecordRequest> WriteCalls;
    TVector<NCloud::NProto::TReadJournalTailRequest> ReadJournalTailCalls;
    TVector<NCloud::NProto::TAdvanceLsnLowWatermarkRequest>
        AdvanceLsnLowWatermarkCalls;
    TVector<NCloud::NProto::TWritePagesRequest> WritePagesCalls;

    NCloud::NProto::TAcquireDevicesResponse AcquireResp;
    NCloud::NProto::TReleaseDevicesResponse ReleaseResp;
    NCloud::NProto::TFormatDeviceResponse FormatResp;
    NCloud::NProto::TListDevicesResponse ListResp;
    NCloud::NProto::TReadPagesResponse ReadResp;
    NCloud::NProto::TWriteLogRecordResponse WriteResp;
    NCloud::NProto::TReadJournalTailResponse ReadJournalTailResp;
    NCloud::NProto::TAdvanceLsnLowWatermarkResponse
        AdvanceLsnLowWatermarkResp;
    NCloud::NProto::TWritePagesResponse WritePagesResp;

    TDeque<NCloud::NProto::TAcquireDevicesResponse> AcquireRespQueue;
    TDeque<NCloud::NProto::TReleaseDevicesResponse> ReleaseRespQueue;
    TDeque<NCloud::NProto::TFormatDeviceResponse> FormatRespQueue;
    TDeque<NCloud::NProto::TListDevicesResponse> ListRespQueue;
    TDeque<NCloud::NProto::TReadPagesResponse> ReadRespQueue;
    TDeque<NCloud::NProto::TWriteLogRecordResponse> WriteRespQueue;
    TDeque<NCloud::NProto::TReadJournalTailResponse> ReadJournalTailRespQueue;
    TDeque<NCloud::NProto::TWritePagesResponse> WritePagesRespQueue;

    NCloud::NProto::TAcquireDevicesResponse AcquireDevices(
        TFastShardContext&,
        NCloud::NProto::TAcquireDevicesRequest request) override;

    NCloud::NProto::TReleaseDevicesResponse ReleaseDevices(
        TFastShardContext&,
        NCloud::NProto::TReleaseDevicesRequest request) override;

    NCloud::NProto::TFormatDeviceResponse FormatDevice(
        TFastShardContext&,
        NCloud::NProto::TFormatDeviceRequest request) override;

    NCloud::NProto::TListDevicesResponse ListDevices(
        TFastShardContext&,
        NCloud::NProto::TListDevicesRequest request) override;

    NCloud::NProto::TReadPagesResponse ReadPages(
        TFastShardContext&,
        NCloud::NProto::TReadPagesRequest request) override;

    NCloud::NProto::TWriteLogRecordResponse WriteLogRecord(
        TFastShardContext&,
        NCloud::NProto::TWriteLogRecordRequest request) override;

    NCloud::NProto::TReadJournalTailResponse ReadJournalTail(
        TFastShardContext&,
        NCloud::NProto::TReadJournalTailRequest request) override;

    NCloud::NProto::TAdvanceLsnLowWatermarkResponse AdvanceLsnLowWatermark(
        TFastShardContext&,
        NCloud::NProto::TAdvanceLsnLowWatermarkRequest request) override;

    NCloud::NProto::TWritePagesResponse WritePages(
        TFastShardContext&,
        NCloud::NProto::TWritePagesRequest request) override;
};

}   // namespace NCloud::NFastShard
