#pragma once

#include "public.h"

#include <cloud/blockstore/libs/daemon/common/bootstrap.h>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

class TBootstrapLocal final
    : public TBootstrapBase
{
private:
    TConfigInitializerLocalPtr Configs;

public:
    TBootstrapLocal(IDeviceHandlerFactoryPtr deviceHandlerFactory);
    ~TBootstrapLocal();

    TProgramShouldContinue& GetShouldContinue() override;

protected:
    TConfigInitializerCommonPtr InitConfigs(int argc, char** argv) override;

    IStartable* GetActorSystem() override        { return nullptr; }
    IStartable* GetAsyncLogger() override        { return nullptr; }
    IStartable* GetStatsAggregator() override    { return nullptr; }
    IStartable* GetClientPercentiles() override  { return nullptr; }
    IStartable* GetStatsUploader() override      { return nullptr; }
    IStartable* GetYdbStorage() override         { return nullptr; }
    IStartable* GetLogbrokerService() override   { return nullptr; }
    IStartable* GetNotifyService() override      { return nullptr; }
    IStartable* GetStatsFetcher() override       { return nullptr; }
    IStartable* GetIamTokenClient() override     { return nullptr; }
    IStartable* GetSyncIamTokenClient() override { return nullptr; }
    IStartable* GetComputeClient() override      { return nullptr; }
    IStartable* GetKmsClient() override          { return nullptr; }
    IStartable* GetRootKmsClient() override      { return nullptr; }
    ITraceSerializerPtr GetTraceSerializer() override;

    ITraceServiceClientPtr GetTraceServiceClient() override
    {
        return nullptr;
    }

    void InitSpdk() override;
    void InitRdmaClient() override;
    void InitRdmaServer() override;
    void InitKikimrService() override;
    void InitAuthService() override;

    void WarmupBSGroupConnections() override;

    void InitRdmaRequestServer() override;

    void SetupCellManager() override;
};

}   // namespace NCloud::NBlockStore::NServer
