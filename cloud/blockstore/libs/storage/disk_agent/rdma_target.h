#pragma once

#include <cloud/blockstore/config/disk.pb.h>
#include <cloud/blockstore/libs/rdma/iface/public.h>
#include <cloud/blockstore/libs/service/storage.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/public.h>

#include <cloud/storage/core/libs/common/startable.h>
#include <cloud/storage/core/libs/diagnostics/public.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TOldRequestCounters
{
    NMonitoring::TDynamicCounters::TCounterPtr Delayed = {};
    NMonitoring::TDynamicCounters::TCounterPtr Rejected = {};
    NMonitoring::TDynamicCounters::TCounterPtr Already = {};
};

struct TRdmaTargetConfig
{
    bool RejectLateRequestsAtDiskAgentEnabled = false;
    TOldRequestCounters OldRequestCounters = {};
    ui32 PoolSize = 1;
};

struct IRdmaTarget: IStartable
{
    virtual NProto::TError DeviceSecureEraseStart(
        const TString& deviceUUID) = 0;
    virtual void DeviceSecureEraseFinish(
        const TString& deviceUUID,
        const NProto::TError& error) = 0;
};

using TStorageAdapterPtr = std::shared_ptr<TStorageAdapter>;
using IRdmaTargetPtr = std::shared_ptr<IRdmaTarget>;

IRdmaTargetPtr CreateRdmaTarget(
    NProto::TRdmaEndpoint config,
    TRdmaTargetConfig rdmaTargetConfig,
    ILoggingServicePtr logging,
    NRdma::IServerPtr server,
    TDeviceClientPtr deviceClient,
    THashMap<TString, TStorageAdapterPtr> devices);

}   // namespace NCloud::NBlockStore::NStorage
