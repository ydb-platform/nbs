#pragma once

#include "public.h"

#include <cloud/filestore/libs/storage/core/public.h>

#include <cloud/storage/core/libs/kikimr/public.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

NActors::IActorPtr CreateIndexTabletProxy(TStorageConfigPtr config);

}   // namespace NCloud::NFileStore::NStorage
