#pragma once

#include "vfs.h"

#include <cloud/filestore/libs/service/context.h>

namespace NCloud::NFileStore::NVFSVhost {

////////////////////////////////////////////////////////////////////////////////

struct TVfsRequestContext
{
    TCallContextPtr CallContext;
    TVfsRequestPtr VfsRequest;

    TVfsRequestContext(TCallContextPtr callContext, TVfsRequestPtr request) noexcept
        : CallContext(std::move(callContext))
        , VfsRequest(std::move(request))
    {}
};

}   // namespace NCloud::NFileStore::NVFSVhost
