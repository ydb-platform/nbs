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
    ITaskQueuePtr backgroundTaskQueue,
    ILoggingServicePtr logging,
    NRdma::IServerPtr rdmaServer,
    NNvme::INvmeManagerPtr nvmeManager)
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
        std::move(backgroundTaskQueue),
        std::move(logging),
        std::move(rdmaServer),
        std::move(nvmeManager));
}

}   // namespace NCloud::NBlockStore::NStorage
