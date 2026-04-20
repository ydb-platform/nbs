#pragma once

#include <cloud/storage/core/libs/actors/public.h>
#include <cloud/storage/core/libs/diagnostics/public.h>
#include <cloud/storage/core/libs/rdma/iface/public.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

NRdma::IClientPtr CreateFakeRdmaClient(IActorSystemPtr actorSystem);

}   // namespace NCloud::NBlockStore
