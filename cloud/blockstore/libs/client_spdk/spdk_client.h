#pragma once

#include "public.h"

#include <cloud/blockstore/libs/service/public.h>
#include <cloud/blockstore/libs/spdk/public.h>

#include <util/generic/string.h>

namespace NCloud::NBlockStore::NClient {

////////////////////////////////////////////////////////////////////////////////

struct TNVMeEndpointConfig
{
    TString DeviceTransportId;
    TString DeviceNqn;
    TString SocketPath;
};

IBlockStorePtr CreateNVMeEndpointClient(
    NSpdk::ISpdkEnvPtr spdk,
    IBlockStorePtr volumeClient,
    const TNVMeEndpointConfig& config);

////////////////////////////////////////////////////////////////////////////////

struct TSCSIEndpointConfig
{
    TString DeviceUrl;
    TString InitiatorIqn;
    TString SocketPath;
};

IBlockStorePtr CreateSCSIEndpointClient(
    NSpdk::ISpdkEnvPtr spdk,
    IBlockStorePtr volumeClient,
    const TSCSIEndpointConfig& config);

}   // namespace NCloud::NBlockStore::NClient
