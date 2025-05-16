#pragma once

#include "aligned_device_handler.h"

#include <cloud/blockstore/libs/service/device_handler.h>

#include <util/generic/list.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

class TModifyRequest;
using TModifyRequestPtr = std::shared_ptr<TModifyRequest>;
using TModifyRequestWeakPtr = std::weak_ptr<TModifyRequest>;
using TModifyRequestIt = TList<TModifyRequestPtr>::iterator;

// The TUnalignedDeviceHandler can handle both aligned and unaligned requests.
// If a request is unaligned, the read-modify-write sequence is used.
// TUnalignedDeviceHandler monitors the execution of all requests. If an
// unaligned request needs to be executed, it waits for all requests it
// intersects to be completed before run. While an unaligned request is being
// processed, it prevents other requests from being processed that overlap with
// it.
// The TAlignedDeviceHandler is used to process requests. Only aligned requests
// are sent to this handler.
class TUnalignedDeviceHandler final
    : public IDeviceHandler
    , public std::enable_shared_from_this<TUnalignedDeviceHandler>
{
private:
    const std::shared_ptr<TAlignedDeviceHandler> Backend;
    const ui32 BlockSize;
    const ui32 MaxUnalignedBlockCount;

    // Requests that are currently in flight. These fields are only accessible
    // under RequestsLock.
    TList<TModifyRequestPtr> AlignedRequests;
    TList<TModifyRequestPtr> UnalignedRequests;
    TAdaptiveLock RequestsLock;

public:
    TUnalignedDeviceHandler(
        IStoragePtr storage,
        TString diskId,
        TString clientId,
        ui32 blockSize,
        ui32 maxSubRequestSize,
        ui32 maxZeroBlocksSubRequestSize,
        ui32 maxUnalignedRequestSize,
        bool checkBufferModificationDuringWriting,
        bool isReliableMediaKind);

    ~TUnalignedDeviceHandler() override;

    NThreading::TFuture<NProto::TReadBlocksLocalResponse> Read(
        TCallContextPtr ctx,
        ui64 from,
        ui64 length,
        TGuardedSgList sgList,
        const TString& checkpointId) override;

    NThreading::TFuture<NProto::TWriteBlocksLocalResponse> Write(
        TCallContextPtr ctx,
        ui64 from,
        ui64 length,
        TGuardedSgList sgList) override;

    NThreading::TFuture<NProto::TZeroBlocksResponse>
    Zero(TCallContextPtr ctx, ui64 from, ui64 length) override;

    TStorageBuffer AllocateBuffer(size_t bytesCount) override;

private:
    // Registers the request in the in-flight lists.
    // If the request needs to be processed immediately, a true value will be
    // returned. This flag should not be disregarded.
    [[nodiscard]] bool RegisterRequest(TModifyRequestPtr request);

    NThreading::TFuture<NProto::TReadBlocksResponse>
    ExecuteUnalignedReadRequest(
        TCallContextPtr ctx,
        TBlocksInfo blocksInfo,
        TGuardedSgList sgList,
        TString checkpointId) const;

    void OnRequestFinished(TModifyRequestWeakPtr weakRequest);
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore
