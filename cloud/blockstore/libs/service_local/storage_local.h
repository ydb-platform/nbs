#pragma once

#include "public.h"

#include <cloud/blockstore/libs/nvme/public.h>
#include <cloud/blockstore/libs/service/public.h>

#include <cloud/storage/core/libs/common/public.h>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

struct TLocalStorageProviderParams
{
    bool DirectIO = false;
    bool UseSubmissionThread = false;
    bool EnableDataIntegrityValidation = false;
    TString SubmissionThreadName = "AIO.SQ";
};

////////////////////////////////////////////////////////////////////////////////

IStorageProviderPtr CreateLocalStorageProvider(
    IFileIOServiceProviderPtr fileIOProvider,
    NNvme::INvmeManagerPtr nvmeManager,
    TLocalStorageProviderParams params);

}   // namespace NCloud::NBlockStore::NServer
