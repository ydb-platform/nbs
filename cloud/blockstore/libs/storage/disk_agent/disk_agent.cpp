#include "disk_agent.h"

#include "disk_agent_actor.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

IActorPtr CreateDiskAgent(
    TStorageConfigPtr config,
    TDiskAgentConfigPtr agentConfig,
    NRdma::TRdmaConfigPtr rdmaConfig,
    NSpdk::ISpdkEnvPtr spdk,
    ICachingAllocatorPtr allocator,
    IStorageProviderPtr storageProvider,
    IProfileLogPtr profileLog,
    IBlockDigestGeneratorPtr blockDigestGenerator,
    ILoggingServicePtr logging,
    NRdma::IServerPtr rdmaServer,
    NNvme::INvmeManagerPtr nvmeManager,
    ITaskQueuePtr backgroundThreadPool,
    ILocalNVMeServicePtr localNVMeService)
{
    return std::make_unique<TDiskAgentActor>(
        std::move(config),
        std::move(agentConfig),
        std::move(rdmaConfig),
        std::move(spdk),
        std::move(allocator),
        std::move(storageProvider),
        std::move(profileLog),
        std::move(blockDigestGenerator),
        std::move(logging),
        std::move(rdmaServer),
        std::move(nvmeManager),
        std::move(backgroundThreadPool),
        std::move(localNVMeService));
}

}   // namespace NCloud::NBlockStore::NStorage
