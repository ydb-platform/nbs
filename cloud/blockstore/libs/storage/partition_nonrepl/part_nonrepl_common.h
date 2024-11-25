#pragma once

#include "public.h"

#include "config.h"

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/protos/disk.pb.h>
#include <cloud/storage/core/libs/kikimr/helpers.h>

#include <contrib/ydb/library/actors/core/actorsystem.h>

#include <util/string/builder.h>

#include <errno.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

inline void SendEvReacquireDisk(
    const NActors::TActorContext& ctx,
    const NActors::TActorId& recipient)
{
    NCloud::Send(
        ctx,
        recipient,
        std::make_unique<TEvVolume::TEvReacquireDisk>()
    );
}

inline void SendEvReacquireDisk(
    NActors::TActorSystem& system,
    const NActors::TActorId& recipient)
{
    auto event = std::make_unique<NActors::IEventHandle>(
        recipient,
        NActors::TActorId{},
        new TEvVolume::TEvReacquireDisk(),
        0,
        0);

    system.Send(event.release());
}

void DeclineGetChangedBlocks(
    const TEvService::TEvGetChangedBlocksRequest::TPtr& ev,
    const NActors::TActorContext& ctx);

////////////////////////////////////////////////////////////////////////////////

template <typename TSystem>
void ProcessError(
    TSystem& system,
    TNonreplicatedPartitionConfig& config,
    NProto::TError& error)
{
    if (error.GetCode() == E_BS_INVALID_SESSION) {
        SendEvReacquireDisk(system, config.GetParentActorId());

        error.SetCode(E_REJECTED);
    }

    if (error.GetCode() == E_IO || error.GetCode() == MAKE_SYSTEM_ERROR(EIO)) {
        error = config.MakeIOError(std::move(*error.MutableMessage()), true);
    }
}

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
    void BuildNextRequest(TSgList* sglist)
    {
        Y_ABORT_UNLESS(CurrentDeviceIdx < DeviceRequests.size());

        const auto& deviceRequest = DeviceRequests[CurrentDeviceIdx];
        for (ui32 i = 0; i < deviceRequest.BlockRange.Size(); ++i) {
            const ui32 rem = Buffer().size() - CurrentOffsetInBuffer;
            Y_ABORT_UNLESS(rem >= BlockSize);
            sglist->push_back(
                {Buffer().data() + CurrentOffsetInBuffer, BlockSize});
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
    TString& Buffer()
    {
        return (*Request.MutableBlocks()->MutableBuffers())[CurrentBufferIdx];
    }
};

////////////////////////////////////////////////////////////////////////////////

inline TString LogDevice(const NProto::TDeviceConfig& device)
{
    return TStringBuilder() << device.GetDeviceUUID()
        << "@" << device.GetAgentId();
}

}   // namespace NCloud::NBlockStore::NStorage
