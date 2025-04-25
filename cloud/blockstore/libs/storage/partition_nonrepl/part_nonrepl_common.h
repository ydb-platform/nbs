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

// Transfers data from a user's TWriteBlocksRequest to subrequests.
// After construction, the BuildNextRequest method is called as many times as
// there are elements in DeviceRequests. The subrequests may not cover all the
// data of the original request, in which case it is necessary to skip and not
// transfer part of the data to the subrequests.
// BlockSize in Request and subrequests is the same. Request buffers may vary
// in size, but they are all multiples of the BlockSize. It is guaranteed that
// there will be enough data to fill out the subrequests. When building
// subrequests, all data will be sliced by BlockSize. If possible, the data from
// the request buffers is moved to the subrequests buffers.

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

        for (size_t i = 0; i < deviceRequest.BlockRange.Size(); ++i) {
            r.MutableBlocks()->AddBuffers(TakeBufferAndAdvance());
        }

        ++CurrentDeviceIdx;
    }

private:
    TString TakeBufferAndAdvance();
    TBlockDataRef GetBufferAndAdvance();
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NStorage
