#include "part_nonrepl_common.h"

#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/storage/core/libs/kikimr/helpers.h>

#include <util/string/builder.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

namespace {

void SendEvReacquireDisk(
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

}   // namespace

void ProcessError(
    NActors::TActorSystem& system,
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

void DeclineGetChangedBlocks(
    const TEvService::TEvGetChangedBlocksRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto response = std::make_unique<TEvService::TEvGetChangedBlocksResponse>(
        MakeError(E_ARGUMENT, "GetChangedBlocks not supported"));
    NCloud::Reply(ctx, *ev, std::move(response));
}

TString LogDevice(const NProto::TDeviceConfig& device)
{
    return TStringBuilder()
           << device.GetDeviceUUID() << "@" << device.GetAgentId();
}

////////////////////////////////////////////////////////////////////////////////

TDeviceRequestBuilder::TDeviceRequestBuilder(
        const TVector<TDeviceRequest>& deviceRequests,
        const ui32 blockSize,
        NProto::TWriteBlocksRequest& request)
    : DeviceRequests(deviceRequests)
    , BlockSize(blockSize)
    , Request(request)
{}

void TDeviceRequestBuilder::BuildNextRequest(TSgList* sglist)
{
    Y_ABORT_UNLESS(CurrentDeviceIdx < DeviceRequests.size());

    const auto& deviceRequest = DeviceRequests[CurrentDeviceIdx];
    for (ui32 i = 0; i < deviceRequest.BlockRange.Size(); ++i) {
        const ui32 rem = Buffer().size() - CurrentOffsetInBuffer;
        Y_ABORT_UNLESS(rem >= BlockSize);
        sglist->push_back({Buffer().data() + CurrentOffsetInBuffer, BlockSize});
        CurrentOffsetInBuffer += BlockSize;
        if (CurrentOffsetInBuffer == Buffer().size()) {
            CurrentOffsetInBuffer = 0;
            ++CurrentBufferIdx;
        }
    }

    ++CurrentDeviceIdx;
}

void TDeviceRequestBuilder::BuildNextRequest(
    NProto::TWriteDeviceBlocksRequest& r)
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

TString& TDeviceRequestBuilder::Buffer()
{
    return (*Request.MutableBlocks()->MutableBuffers())[CurrentBufferIdx];
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NStorage
