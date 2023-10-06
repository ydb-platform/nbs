#pragma once

#include <cloud/filestore/libs/service/public.h>

#include <cloud/storage/core/libs/actors/public.h>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

IAuthProviderPtr CreateKikimrAuthProvider(IActorSystemPtr actorSystem);

}   // namespace NCloud::NFileStore
