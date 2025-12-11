#pragma once

#include "private.h"

#include <cloud/blockstore/libs/rdma/iface/public.h>
#include <cloud/blockstore/libs/service/public.h>
#include <cloud/blockstore/tools/testing/rdma-test/protocol.pb.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/guarded_sglist.h>
#include <cloud/storage/core/libs/common/public.h>
#include <cloud/storage/core/libs/common/startable.h>

#include <library/cpp/lwtrace/shuttle.h>
#include <library/cpp/threading/future/future.h>

#include <util/generic/strbuf.h>
#include <util/generic/string.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

using TReadBlocksRequestPtr = std::shared_ptr<NProto::TReadBlocksRequest>;
using TWriteBlocksRequestPtr = std::shared_ptr<NProto::TWriteBlocksRequest>;

////////////////////////////////////////////////////////////////////////////////

struct IStorage: public IStartable
{
    virtual ~IStorage() = default;

    virtual NThreading::TFuture<NProto::TError> ReadBlocks(
        TCallContextPtr callContext,
        TReadBlocksRequestPtr request,
        TGuardedSgList sglist) = 0;

    virtual NThreading::TFuture<NProto::TError> WriteBlocks(
        TCallContextPtr callContext,
        TWriteBlocksRequestPtr request,
        TGuardedSgList sglist) = 0;

    virtual TStorageBuffer AllocateBuffer(size_t len);
};

////////////////////////////////////////////////////////////////////////////////

ui64 CreateRequestId();

IStoragePtr CreateNullStorage();

IStoragePtr CreateMemoryStorage(ui32 blockSize, ui32 blocksCount);

IStoragePtr
CreateLocalAIOStorage(const TString& path, ui32 blockSize, ui32 blocksCount);

IStoragePtr
CreateLocalURingStorage(const TString& path, ui32 blockSize, ui32 blocksCount);

IStoragePtr CreateRdmaStorage(
    NRdma::IClientPtr client,
    ITaskQueuePtr taskQueue,
    const TString& address,
    ui32 port,
    TDuration timeout);

size_t CopyMemory(const TSgList& dst, TStringBuf src);
size_t CopyMemory(TStringBuf dst, const TSgList& src);

}   // namespace NCloud::NBlockStore
