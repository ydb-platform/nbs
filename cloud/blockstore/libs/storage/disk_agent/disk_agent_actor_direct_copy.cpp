#include "disk_agent_actor.h"

#include <cloud/blockstore/libs/storage/disk_agent/actors/direct_copy_actor.h>
namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

void TDiskAgentActor::HandleDirectCopyBlocks(
    const TEvDiskAgent::TEvDirectCopyBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();
    auto& record = msg->Record;

    LOG_TRACE(
        ctx,
        TBlockStoreComponents::DISK_AGENT,
        "DirectCopyBlocks received, SourceUUID=%s %s, TargetUUID=%s %s",
        record.GetSourceDeviceUUID().Quote().c_str(),
        DescribeRange(TBlockRange64::WithLength(
                          record.GetSourceStartIndex(),
                          record.GetBlockCount()))
            .c_str(),
        record.GetTargetDeviceUUID().Quote().c_str(),
        DescribeRange(TBlockRange64::WithLength(
                          record.GetTargetStartIndex(),
                          record.GetBlockCount()))
            .c_str());

    ui64 recommendedBandwidth = BandwidthCalculator.RegisterRequest(
        record.GetSourceDeviceUUID(),
        ctx.Now());

    NCloud::Register<TDirectCopyActor>(
        ctx,
        SelfId(),
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext),
        std::move(record),
        recommendedBandwidth);
}

}   // namespace NCloud::NBlockStore::NStorage
