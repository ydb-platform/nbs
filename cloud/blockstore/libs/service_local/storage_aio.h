#pragma once

#include "public.h"

#include <cloud/blockstore/libs/nvme/public.h>
#include <cloud/blockstore/libs/service/public.h>

#include <cloud/storage/core/libs/common/public.h>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

enum class EAioSubmitQueueOpt : bool {
    DontUse = false,
    Use = true
};

////////////////////////////////////////////////////////////////////////////////

IStorageProviderPtr CreateAioStorageProvider(
    IFileIOServiceProviderPtr fileIOProvider,
    NNvme::INvmeManagerPtr nvmeManager,
    bool directIO,
    EAioSubmitQueueOpt submitQueueOpt);

}   // namespace NCloud::NBlockStore::NServer
