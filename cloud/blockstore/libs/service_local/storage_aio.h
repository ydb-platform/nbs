#pragma once

#include "public.h"

#include <cloud/blockstore/libs/nvme/public.h>
#include <cloud/blockstore/libs/service/public.h>

#include <cloud/storage/core/libs/common/public.h>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

IStorageProviderPtr CreateAioStorageProvider(
    IFileIOServicePtr fileIO,
    NNvme::INvmeManagerPtr nvmeManager,
    bool directIO);

IStorageProviderPtr CreateAioStorageProvider(
    IFileIOServicePtr fileIO,
    ITaskQueuePtr submissionQueue,
    ITaskQueuePtr completionQueue,
    NNvme::INvmeManagerPtr nvmeManager,
    bool directIO);

}   // namespace NCloud::NBlockStore::NServer
