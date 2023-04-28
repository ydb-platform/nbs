#pragma once

#include "public.h"

#include <cloud/blockstore/libs/kikimr/public.h>
#include <cloud/blockstore/libs/service/public.h>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

IBlockStorePtr CreateKikimrService(
    IActorSystemPtr actorSystem,
    const NProto::TKikimrServiceConfig& config);

}   // namespace NCloud::NBlockStore::NServer
