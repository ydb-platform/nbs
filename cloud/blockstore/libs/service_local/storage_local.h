#pragma once

#include "public.h"

#include <cloud/blockstore/libs/nvme/public.h>
#include <cloud/blockstore/libs/service/public.h>
#include <cloud/blockstore/public/api/protos/disk.pb.h>

#include <cloud/storage/core/libs/common/public.h>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

struct TLocalStorageProviderParams
{
    bool DirectIO = false;
    bool UseSubmissionThread = false;
    ui32 ValidatedBlocksRatio = 1000;
    NProto::EDataIntegrityValidationPolicy DataIntegrityValidationPolicy =
        NProto::DIVP_DISABLED;
    TString SubmissionThreadName = "AIO.SQ";
};

////////////////////////////////////////////////////////////////////////////////

IStorageProviderPtr CreateLocalStorageProvider(
    IFileIOServiceProviderPtr fileIOProvider,
    NNvme::INvmeManagerPtr nvmeManager,
    TLocalStorageProviderParams params);

}   // namespace NCloud::NBlockStore::NServer
