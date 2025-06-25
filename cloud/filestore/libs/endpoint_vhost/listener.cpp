#include "listener.h"

#include <cloud/filestore/libs/client/config.h>
#include <cloud/filestore/libs/client/session.h>
#include <cloud/filestore/libs/endpoint/listener.h>
#include <cloud/filestore/libs/service/filestore.h>
#include <cloud/filestore/libs/vfs/config.h>
#include <cloud/filestore/libs/vfs/loop.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

namespace NCloud::NFileStore::NVhost {

using namespace NThreading;
using namespace NCloud::NFileStore::NClient;
using namespace NCloud::NFileStore::NVFS;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TEndpoint final
    : public IEndpoint
{
private:
    const IFileSystemLoopPtr Loop;

public:
    TEndpoint(IFileSystemLoopPtr loop)
        : Loop(std::move(loop))
    {}

    TFuture<NProto::TError> StartAsync() override
    {
        return Loop->StartAsync();
    }

    TFuture<void> StopAsync() override
    {
        return Loop->StopAsync().IgnoreResult();
    }

    TFuture<void> SuspendAsync() override
    {
        return Loop->SuspendAsync().IgnoreResult();
    }

    TFuture<NProto::TError> AlterAsync(
        bool isReadonly,
        ui64 mountSeqNumber) override
    {
        return Loop->AlterAsync(isReadonly, mountSeqNumber);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TEndpointListener final
    : public IEndpointListener
{
private:
    const ILoggingServicePtr Logging;
    const ITimerPtr Timer;
    const ISchedulerPtr Scheduler;
    const IFileStoreEndpointsPtr FileStoreEndpoints;
    const IFileSystemLoopFactoryPtr LoopFactory;
    const THandleOpsQueueConfig HandleOpsQueueConfig;
    const TWriteBackCacheConfig WriteBackCacheConfig;

    TLog Log;

public:
    TEndpointListener(
            ILoggingServicePtr logging,
            ITimerPtr timer,
            ISchedulerPtr scheduler,
            IFileStoreEndpointsPtr filestoreEndpoints,
            IFileSystemLoopFactoryPtr loopFactory,
            THandleOpsQueueConfig handleOpsQueueConfig,
            TWriteBackCacheConfig writeBackCacheConfig)
        : Logging(std::move(logging))
        , Timer(std::move(timer))
        , Scheduler(std::move(scheduler))
        , FileStoreEndpoints(std::move(filestoreEndpoints))
        , LoopFactory(std::move(loopFactory))
        , HandleOpsQueueConfig(std::move(handleOpsQueueConfig))
        , WriteBackCacheConfig(std::move(writeBackCacheConfig))
    {
        Log = Logging->CreateLog("NFS_VHOST");
    }

    IEndpointPtr CreateEndpoint(const NProto::TEndpointConfig& config) override
    {
        const auto& serviceEndpoint = config.GetServiceEndpoint();

        auto filestore = FileStoreEndpoints->GetEndpoint(serviceEndpoint);
        if (!filestore) {
            ythrow TServiceError(E_ARGUMENT)
                << "invalid service endpoint " << serviceEndpoint.Quote();
        }

        NProto::TSessionConfig sessionConfig;
        sessionConfig.SetFileSystemId(config.GetFileSystemId());
        sessionConfig.SetClientId(config.GetClientId());
        sessionConfig.SetSessionPingTimeout(config.GetSessionPingTimeout());
        sessionConfig.SetSessionRetryTimeout(config.GetSessionRetryTimeout());

        auto session = CreateSession(
            Logging,
            Timer,
            Scheduler,
            std::move(filestore),
            std::make_shared<TSessionConfig>(sessionConfig));

        NProto::TVFSConfig protoConfig;
        protoConfig.SetFileSystemId(config.GetFileSystemId());
        protoConfig.SetClientId(config.GetClientId());
        protoConfig.SetSocketPath(config.GetSocketPath());
        protoConfig.SetReadOnly(config.GetReadOnly());
        protoConfig.SetMountSeqNumber(config.GetMountSeqNumber());
        protoConfig.SetVhostQueuesCount(config.GetVhostQueuesCount());
        if (Log.FiltrationLevel() >= TLOG_DEBUG) {
            protoConfig.SetDebug(true);
        }
        protoConfig.SetHandleOpsQueuePath(HandleOpsQueueConfig.PathPrefix);
        protoConfig.SetHandleOpsQueueSize(HandleOpsQueueConfig.MaxQueueSize);
        protoConfig.SetWriteBackCachePath(WriteBackCacheConfig.PathPrefix);
        protoConfig.SetWriteBackCacheCapacity(WriteBackCacheConfig.Capacity);
        protoConfig.SetWriteBackCacheAutomaticFlushPeriod(
            WriteBackCacheConfig.AutomaticFlushPeriod.MilliSeconds());

        auto vFSConfig = std::make_shared<TVFSConfig>(std::move(protoConfig));
        auto Loop = LoopFactory->Create(
            std::move(vFSConfig),
            std::move(session));

        return std::make_shared<TEndpoint>(std::move(Loop));
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IEndpointListenerPtr CreateEndpointListener(
    ILoggingServicePtr logging,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    IFileStoreEndpointsPtr filestoreEndpoints,
    IFileSystemLoopFactoryPtr loopFactory,
    THandleOpsQueueConfig handleOpsQueueConfig,
    TWriteBackCacheConfig writeBackCacheConfig)
{
    return std::make_shared<TEndpointListener>(
        std::move(logging),
        std::move(timer),
        std::move(scheduler),
        std::move(filestoreEndpoints),
        std::move(loopFactory),
        std::move(handleOpsQueueConfig),
        std::move(writeBackCacheConfig));
}

}   // namespace NCloud::NFileStore::NVhost
