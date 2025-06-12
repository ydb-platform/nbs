#pragma once

#include "public.h"

#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/discovery/public.h>
#include <cloud/blockstore/libs/encryption/public.h>
#include <cloud/blockstore/libs/endpoints/public.h>
#include <cloud/blockstore/libs/kikimr/public.h>
#include <cloud/blockstore/libs/rdma/iface/public.h>
#include <cloud/blockstore/libs/storage/core/public.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

NActors::IActorPtr CreateStorageService(
    TStorageConfigPtr config,
    TDiagnosticsConfigPtr diagnosticsConfig,
    IProfileLogPtr profileLog,
    IBlockDigestGeneratorPtr blockDigestGenerator,
    NDiscovery::IDiscoveryServicePtr discoveryService,
    ITraceSerializerPtr traceSerializer,
    NServer::IEndpointEventHandlerPtr endpointEventHandler,
    NRdma::IClientPtr rdmaClient,
    IVolumeStatsPtr volumeStats,
    TManuallyPreemptedVolumesPtr preemptedVolumes,
    IRootKmsKeyProviderPtr rootKmsKeyProvider,
    bool temporaryServer);

}   // namespace NCloud::NBlockStore::NStorage
