#pragma once

#include "public.h"

#include "config.h"

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/protos/disk.pb.h>

#include <library/cpp/actors/core/actorsystem.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

void ProcessError(
    const NActors::TActorSystem& system,
    const TNonreplicatedPartitionConfig& config,
    NProto::TError& error);

void DeclineGetChangedBlocks(
    const TEvService::TEvGetChangedBlocksRequest::TPtr& ev,
    const NActors::TActorContext& ctx);

TString LogDevice(const NProto::TDeviceConfig& device);

////////////////////////////////////////////////////////////////////////////////

class TDeviceRequestBuilder
{
private:
    const TVector<TDeviceRequest>& DeviceRequests;
    const ui32 BlockSize;
    NProto::TWriteBlocksRequest& Request;
    ui32 CurrentDeviceIdx = 0;
    ui32 CurrentBufferIdx = 0;
    ui32 CurrentOffsetInBuffer = 0;

public:
    TDeviceRequestBuilder(
        const TVector<TDeviceRequest>& deviceRequests,
        const ui32 blockSize,
        NProto::TWriteBlocksRequest& request);

    void BuildNextRequest(TSgList* sglist);

    void BuildNextRequest(NProto::TWriteDeviceBlocksRequest& r);

private:
    TString& Buffer();
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NStorage
