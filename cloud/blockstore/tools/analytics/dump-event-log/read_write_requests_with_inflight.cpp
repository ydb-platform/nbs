#include "read_write_requests_with_inflight.h"

#include <cloud/blockstore/libs/service/request.h>

#include <util/stream/file.h>
#include <util/string/builder.h>

namespace NCloud::NBlockStore {

///////////////////////////////////////////////////////////////////////////////

TReadWriteRequestsWithInflight::TReadWriteRequestsWithInflight(
    const TString& filename)
    : Output(filename)
{}

TReadWriteRequestsWithInflight::~TReadWriteRequestsWithInflight()
{
    Output.Flush();
}

void TReadWriteRequestsWithInflight::ProcessRequest(
    const TDiskInfo& diskInfo,
    TInstant timestamp,
    ui32 requestType,
    TBlockRange64 blockRange,
    TDuration duration,
    TDuration postponed,
    const TReplicaChecksums& replicaChecksums,
    const TInflightInfo& inflightInfo)
{
    Y_UNUSED(replicaChecksums);
    Y_UNUSED(blockRange);

    if (!IsReadRequestType(requestType) && !IsWriteRequestType(requestType)) {
        return;
    }

    TStringBuilder sb;
    sb << "{" << diskInfo.DiskId.Quote();
    sb << "," << requestType;
    sb << "," << timestamp.MicroSeconds();
    sb << "," << postponed.MicroSeconds();
    sb << "," << duration.MicroSeconds();
    sb << "," << blockRange.Size() * diskInfo.BlockSize;
    sb << "," << inflightInfo.HostBlobStorageReadInflight;
    sb << "," << inflightInfo.HostBlobStorageReadInflightByteCount;
    sb << "," << inflightInfo.HostBlobStorageWriteInflight;
    sb << "," << inflightInfo.HostBlobStorageWriteInflightByteCount;
    sb << "," << inflightInfo.HostDiskRegistryReadInflight;
    sb << "," << inflightInfo.HostDiskRegistryReadInflightByteCount;
    sb << "," << inflightInfo.HostDiskRegistryWriteInflight;
    sb << "," << inflightInfo.HostDiskRegistryWriteInflightByteCount;
    sb << "}\n";
    Output.Write(sb);
}

}   // namespace NCloud::NBlockStore
