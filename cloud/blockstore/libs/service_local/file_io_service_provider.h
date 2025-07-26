#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/public.h>
#include <cloud/storage/core/libs/common/startable.h>

#include <util/generic/strbuf.h>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

struct IFileIOServiceProvider
    : IStartable
{
    virtual IFileIOServicePtr CreateFileIOService(TStringBuf filePath) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IFileIOServiceProviderPtr CreateSingleFileIOServiceProvider(
    IFileIOServicePtr fileIO);

IFileIOServiceProviderPtr CreateFileIOServiceProvider(
    ui32 filePathsPerServices,
    IFileIOServiceFactoryPtr factory);

}   // namespace NCloud::NBlockStore::NServer
