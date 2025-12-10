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
    const TTimeData& timeData,
    ui32 requestType,
    TBlockRange64 blockRange,
    const TReplicaChecksums& replicaChecksums,
    const TInflightData& inflightData)
{
    Y_UNUSED(replicaChecksums);
    Y_UNUSED(blockRange);

    if (!IsReadRequestType(requestType) && !IsWriteRequestType(requestType)) {
        return;
    }

    TStringBuilder sb;

    auto printInflight =
        [](TStringBuilder& sb, const TInflightCounters& inflightCounters)
    {
        sb << "," << inflightCounters.Read.InflightCount;
        sb << "," << inflightCounters.Read.InflightBytes;
        sb << "," << inflightCounters.Write.InflightCount;
        sb << "," << inflightCounters.Write.InflightBytes;
    };

    sb << "[" << diskInfo.DiskId.Quote();
    sb << "," << requestType;
    sb << "," << timeData.StartAt.MicroSeconds();
    sb << "," << timeData.Postponed.MicroSeconds();
    sb << "," << timeData.ExecutionTime.MicroSeconds();
    sb << "," << blockRange.Size() * diskInfo.BlockSize;
    printInflight(sb, inflightData.HostBlobStorageBased);
    printInflight(sb, inflightData.HostDiskRegistryBased);
    sb << "]\n";
    Output.Write(sb);
}

}   // namespace NCloud::NBlockStore
