#pragma once

#include "public.h"

#include <cloud/blockstore/libs/daemon/common/config_initializer.h>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

struct TConfigInitializerLocal final
    : public TConfigInitializerCommon
{
    TOptionsLocalPtr Options;

    TConfigInitializerLocal(TOptionsLocalPtr options);

    ui32 GetLogDefaultLevel() const override;
    ui32 GetMonitoringPort() const override;
    TString GetMonitoringAddress() const override;
    ui32 GetMonitoringThreads() const override;
    bool GetUseNonreplicatedRdmaActor() const override;
    TDuration GetInactiveClientsTimeout() const override;
    TString GetLogBackendFileName() const override;
};

}   // namespace NCloud::NBlockStore::NServer
