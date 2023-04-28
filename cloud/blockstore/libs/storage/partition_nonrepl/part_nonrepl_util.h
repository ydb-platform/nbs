#pragma once

#include "public.h"

#include "config.h"

#include <cloud/blockstore/libs/common/block_range.h>

#include <cloud/blockstore/libs/storage/protos/disk.pb.h>

namespace NCloud::NBlockStore::NStorage {

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
            NProto::TWriteBlocksRequest& request)
        : DeviceRequests(deviceRequests)
        , BlockSize(blockSize)
        , Request(request)
    {
    }

public:
    void BuildNextRequest(TVector<IOutputStream::TPart>* parts)
    {
        Y_VERIFY(CurrentDeviceIdx < DeviceRequests.size());

        const auto& deviceRequest = DeviceRequests[CurrentDeviceIdx];
        for (ui32 i = 0; i < deviceRequest.BlockRange.Size(); ++i) {
            const ui32 rem = Buffer().size() - CurrentOffsetInBuffer;
            Y_VERIFY(rem >= BlockSize);
            parts->push_back({
                Buffer().data() + CurrentOffsetInBuffer,
                BlockSize
            });
            CurrentOffsetInBuffer += BlockSize;
            if (CurrentOffsetInBuffer == Buffer().size()) {
                CurrentOffsetInBuffer = 0;
                ++CurrentBufferIdx;
            }
        }

        ++CurrentDeviceIdx;
    }

    void BuildNextRequest(NProto::TWriteDeviceBlocksRequest& r)
    {
        Y_VERIFY(CurrentDeviceIdx < DeviceRequests.size());

        const auto& deviceRequest = DeviceRequests[CurrentDeviceIdx];
        for (ui32 i = 0; i < deviceRequest.BlockRange.Size(); ++i) {
            auto& deviceBuffer = *r.MutableBlocks()->AddBuffers();
            if (CurrentOffsetInBuffer == 0 && Buffer().size() == BlockSize) {
                deviceBuffer = std::move(Buffer());
                ++CurrentBufferIdx;
            } else {
                const ui32 rem = Buffer().size() - CurrentOffsetInBuffer;
                Y_VERIFY(rem >= BlockSize);
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
    TString& Buffer()
    {
        return (*Request.MutableBlocks()->MutableBuffers())[CurrentBufferIdx];
    }
};


}   // namespace NCloud::NBlockStore::NStorage
