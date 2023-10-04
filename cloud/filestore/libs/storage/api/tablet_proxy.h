#pragma once

#include "public.h"

#include "components.h"
#include "events.h"

#include <cloud/filestore/libs/service/filestore.h>

#include <library/cpp/actors/core/actorid.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

NActors::TActorId MakeIndexTabletProxyServiceId();

}   // namespace NCloud::NFileStore::NStorage
