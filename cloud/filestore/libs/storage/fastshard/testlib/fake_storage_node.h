#pragma once

#include <cloud/filestore/libs/storage/fastshard/sn/iface/storage_node.h>

#include <cloud/storage/core/protos/device.pb.h>

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
 */
struct TFakeStorageNode: public IStorageNode
{
    TAdaptiveLock Lock;

    TVector<NCloud::NProto::TAcquireDevicesRequest> AcquireCalls;
    TVector<NCloud::NProto::TReleaseDevicesRequest> ReleaseCalls;
    TVector<NCloud::NProto::TReadPagesRequest> ReadCalls;
    TVector<NCloud::NProto::TWriteLogRecordRequest> WriteCalls;

    NCloud::NProto::TAcquireDevicesResponse AcquireResp;
    NCloud::NProto::TReleaseDevicesResponse ReleaseResp;
    NCloud::NProto::TReadPagesResponse ReadResp;
    NCloud::NProto::TWriteLogRecordResponse WriteResp;

    NCloud::NProto::TAcquireDevicesResponse AcquireDevices(
        NCloud::NProto::TAcquireDevicesRequest request) override;

    NCloud::NProto::TReleaseDevicesResponse ReleaseDevices(
        NCloud::NProto::TReleaseDevicesRequest request) override;

    NCloud::NProto::TReadPagesResponse ReadPages(
        NCloud::NProto::TReadPagesRequest request) override;

    NCloud::NProto::TWriteLogRecordResponse WriteLogRecord(
        NCloud::NProto::TWriteLogRecordRequest request) override;
};

}   // namespace NCloud::NFileStore::NStorage::NFastShard
