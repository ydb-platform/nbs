#pragma once

#include <cloud/blockstore/libs/nvme/public.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/public.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/string.h>
#include <util/system/file.h>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

NThreading::TFuture<NProto::TError> SafeDeallocateDevice(
    TString filename,
    TFileHandle fd,
    IFileIOServicePtr fileIO,
    ui64 startIndex,
    ui64 blocksCount,
    ui32 blockSize,
    NNvme::INvmeManagerPtr nvmeManager,
    ui32 validatedBlocksRatio   //    1000 means every thousandth or
                                //    0.1% of blocks in device
);

}   // namespace NCloud::NBlockStore::NServer
