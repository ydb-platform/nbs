#pragma once

#include "public.h"

#include <cloud/filestore/libs/daemon/common/bootstrap.h>
#include <cloud/filestore/libs/server/public.h>
#include <cloud/filestore/libs/service/public.h>
#include <cloud/filestore/libs/storage/fastshard/server/server.h>

#include <cloud/storage/core/libs/common/public.h>
#include <cloud/storage/core/libs/rdma/iface/public.h>

namespace NCloud::NFileStore::NDaemon {

////////////////////////////////////////////////////////////////////////////////

class TBootstrapServer final
    : public TBootstrapCommon
{
private:
    TConfigInitializerServerPtr Configs;

    NServer::IServerPtr Server;
    IFileStoreServicePtr Service;
    ITaskQueuePtr ThreadPool;
    NCloud::NStorage::NRdma::IServerPtr RdmaServer;
    IStartablePtr RdmaTarget;

public:
    TBootstrapServer(
        std::shared_ptr<NKikimr::TModuleFactories> moduleFactories);
    ~TBootstrapServer();

    TConfigInitializerCommonPtr InitConfigs(
        int argc,
        char** argv) override;

protected:
    void InitActorSystemPrerequisites() override;
    void InitComponents() override;
    void StartComponents() override;
    void Drain() override;
    void StopComponents() override;

private:
    void InitConfigs();
    void InitLWTrace();
    void InitKikimrService();
    void InitRdmaTarget();
    void InitLocalService();
    void InitNullService();
};

}   // namespace NCloud::NFileStore::NServer
