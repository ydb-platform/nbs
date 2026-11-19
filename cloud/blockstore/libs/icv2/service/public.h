#pragma once

#include <cloud/blockstore/libs/icv2/iface/public.h>

#include <cloud/storage/core/libs/icv2/peer/peer_manager.h>

#include <memory>

namespace NCloud::NBlockStore::NIcV2 {

using NCloud::NIcV2::CreatePeerManager;
using NCloud::NIcV2::EPeerState;
using NCloud::NIcV2::IChannelFactory;
using NCloud::NIcV2::IChannelFactoryPtr;
using NCloud::NIcV2::IPeer;
using NCloud::NIcV2::IPeerManager;
using NCloud::NIcV2::IPeerManagerPtr;
using NCloud::NIcV2::IPeerPtr;
using NCloud::NIcV2::TChannelFuture;
using NCloud::NIcV2::TChannelResult;
using NCloud::NIcV2::TPeerManagerConfig;
using NCloud::NIcV2::TPeerStatus;

struct IService;
using IServicePtr = std::shared_ptr<IService>;

}   // namespace NCloud::NBlockStore::NIcV2
