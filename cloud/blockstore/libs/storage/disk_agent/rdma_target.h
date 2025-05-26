#pragma once

#include <cloud/blockstore/config/disk.pb.h>
#include <cloud/blockstore/libs/rdma/iface/public.h>
#include <cloud/blockstore/libs/service/storage.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/public.h>

#include <cloud/storage/core/libs/common/startable.h>
#include <cloud/storage/core/libs/diagnostics/public.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/system/hostname.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class IMultiAgentWriteHandler;

struct TOldRequestCounters
{
    NMonitoring::TDynamicCounters::TCounterPtr Delayed = {};
    NMonitoring::TDynamicCounters::TCounterPtr Rejected = {};
    NMonitoring::TDynamicCounters::TCounterPtr Already = {};
};

struct TRdmaTargetConfig
{
    bool RejectLateRequests = false;
    TString Host = "::";
    ui32 Port = 10020;
    ui32 WorkerThreads = 1;

    TRdmaTargetConfig(bool rejectLateRequests, NProto::TRdmaTarget target)
        : RejectLateRequests(rejectLateRequests)
    {
        auto& endpoint = target.GetEndpoint();

        if (auto& host = endpoint.GetHost()) {
            Host = host;
        }

        if (auto port = endpoint.GetPort()) {
            Port = port;
        }

        if (auto threads = target.GetWorkerThreads()) {
            WorkerThreads = threads;
        }
    }
};

using TRdmaTargetConfigPtr = std::shared_ptr<TRdmaTargetConfig>;

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
using IMultiAgentWriteHandlerPtr = std::shared_ptr<IMultiAgentWriteHandler>;

IRdmaTargetPtr CreateRdmaTarget(
    TRdmaTargetConfigPtr rdmaTargetConfig,
    TOldRequestCounters oldRequestCounters,
    ILoggingServicePtr logging,
    NRdma::IServerPtr server,
    TDeviceClientPtr deviceClient,
    IMultiAgentWriteHandlerPtr multiAgentWriteHandler,
    THashMap<TString, TStorageAdapterPtr> devices);

}   // namespace NCloud::NBlockStore::NStorage
