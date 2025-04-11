#pragma once

#include <cloud/blockstore/libs/rdma/iface/public.h>
#include <cloud/storage/core/libs/actors/public.h>
#include <cloud/storage/core/libs/diagnostics/public.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

NRdma::IClientPtr CreateFakeRdmaClient(IActorSystemPtr actorSystem);

}   // namespace NCloud::NBlockStore
