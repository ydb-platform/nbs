#pragma once

#include "public.h"

#include "config.h"

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/protos/disk.pb.h>

#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/actors/core/actorsystem.h>

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

    template <typename TRequest>
    void BuildNextRequest(TRequest& r)
    {
        Y_ABORT_UNLESS(CurrentDeviceIdx < DeviceRequests.size());

        const auto& deviceRequest = DeviceRequests[CurrentDeviceIdx];
        for (ui32 i = 0; i < deviceRequest.BlockRange.Size(); ++i) {
            auto& deviceBuffer = *r.MutableBlocks()->AddBuffers();
            if (CurrentOffsetInBuffer == 0 && Buffer().size() == BlockSize) {
                deviceBuffer = std::move(Buffer());
                ++CurrentBufferIdx;
            } else {
                const ui32 rem = Buffer().size() - CurrentOffsetInBuffer;
                Y_ABORT_UNLESS(rem >= BlockSize);
                deviceBuffer.resize(BlockSize);
                memcpy(
                    deviceBuffer.begin(),
                    Buffer().data() + CurrentOffsetInBuffer,
                    BlockSize);
                CurrentOffsetInBuffer += BlockSize;
                if (CurrentOffsetInBuffer == Buffer().size()) {
                    CurrentOffsetInBuffer = 0;
                    ++CurrentBufferIdx;
                }
            }
        }

        ++CurrentDeviceIdx;
    }

private:
    TString& Buffer();
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NStorage
