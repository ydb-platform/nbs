#pragma once

#include "public.h"

#include <cloud/filestore/libs/service/public.h>

#include <cloud/storage/core/libs/kikimr/public.h>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

IFileStoreServicePtr CreateKikimrFileStore(
    IActorSystemPtr actorSystem,
    bool writeBackCacheEnabled);

}   // namespace NCloud::NFileStore
