#pragma once

#include <cloud/blockstore/libs/kikimr/public.h>
#include <cloud/blockstore/libs/service/public.h>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

IAuthProviderPtr CreateKikimrAuthProvider(IActorSystemPtr actorSystem);

}   // namespace NCloud::NBlockStore::NServer
