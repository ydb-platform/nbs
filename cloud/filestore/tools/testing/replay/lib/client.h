#pragma once

#include "public.h"

#include <cloud/filestore/libs/service/public.h>

#include <cloud/storage/core/libs/common/startable.h>

namespace NCloud::NFileStore::NReplay {

////////////////////////////////////////////////////////////////////////////////

struct IClientFactory
    : public IStartable
{
    virtual ~IClientFactory() = default;

    virtual IFileStoreServicePtr CreateClient() = 0;
};

}   // namespace NCloud::NFileStore::NReplay
