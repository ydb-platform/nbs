#include "part_nonrepl_common.h"

#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/storage/core/libs/kikimr/helpers.h>

#include <util/string/builder.h>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

void SendEvReacquireDisk(
    const NActors::TActorSystem& system,
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
    const NActors::TActorSystem& system,
    const TNonreplicatedPartitionConfig& config,
    NProto::TError& error)
{
    if (error.GetCode() == E_BS_INVALID_SESSION) {
        SendEvReacquireDisk(system, config.GetParentActorId());
        error.SetCode(E_REJECTED);
    }

    if (error.GetCode() == E_IO || error.GetCode() == MAKE_SYSTEM_ERROR(EIO)) {
        error = config.MakeIOError(std::move(*error.MutableMessage()));
    } else {
        config.AugmentErrorFlags(error);
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
{
    const size_t blockCountToSkip =
        DeviceRequests[0].BlockRange.Start - request.GetStartIndex();
    for (size_t i = 0; i < blockCountToSkip; ++i) {
        GetBufferAndAdvance();
    }
}

void TDeviceRequestBuilder::BuildNextRequest(TSgList* sglist)
{
    Y_ABORT_UNLESS(CurrentDeviceIdx < DeviceRequests.size());

    const auto& deviceRequest = DeviceRequests[CurrentDeviceIdx];
    sglist->reserve(sglist->size() + deviceRequest.BlockRange.Size());
    for (size_t i = 0; i < deviceRequest.BlockRange.Size(); ++i) {
        sglist->push_back(GetBufferAndAdvance());
    }

    ++CurrentDeviceIdx;
}

TString TDeviceRequestBuilder::TakeBufferAndAdvance() {
    auto& currentBuffer =
        (*Request.MutableBlocks()->MutableBuffers())[CurrentBufferIdx];

    TString result;
    if (CurrentOffsetInBuffer == 0 && currentBuffer.size() == BlockSize) {
        currentBuffer.swap(result);
        ++CurrentBufferIdx;
    } else {
        Y_ABORT_UNLESS(
            currentBuffer.size() - CurrentOffsetInBuffer >= BlockSize);

        result.resize(BlockSize);
        memcpy(
            result.begin(),
            currentBuffer.data() + CurrentOffsetInBuffer,
            BlockSize);
        CurrentOffsetInBuffer += BlockSize;
        if (CurrentOffsetInBuffer == currentBuffer.size()) {
            CurrentOffsetInBuffer = 0;
            ++CurrentBufferIdx;
        }
    }
    return result;
}

TBlockDataRef TDeviceRequestBuilder::GetBufferAndAdvance() {
    auto& currentBuffer =
        (*Request.MutableBlocks()->MutableBuffers())[CurrentBufferIdx];

    Y_ABORT_UNLESS(currentBuffer.size() - CurrentOffsetInBuffer >= BlockSize);

    TBlockDataRef result(
        currentBuffer.data() + CurrentOffsetInBuffer,
        BlockSize);

    CurrentOffsetInBuffer += BlockSize;
    if (CurrentOffsetInBuffer == currentBuffer.size()) {
        CurrentOffsetInBuffer = 0;
        ++CurrentBufferIdx;
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NStorage
