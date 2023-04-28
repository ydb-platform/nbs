#pragma once

#include <cloud/blockstore/config/disk.pb.h>
#include <cloud/blockstore/libs/rdma/public.h>
#include <cloud/blockstore/libs/service/storage.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/public.h>

#include <cloud/storage/core/libs/common/startable.h>
#include <cloud/storage/core/libs/diagnostics/public.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct IRdmaTarget: IStartable {};

using TStorageAdapterPtr = std::shared_ptr<TStorageAdapter>;
using IRdmaTargetPtr = std::shared_ptr<IRdmaTarget>;

IRdmaTargetPtr CreateRdmaTarget(
    NProto::TRdmaEndpoint config,
    ILoggingServicePtr logging,
    NRdma::IServerPtr server,
    TDeviceClientPtr deviceClient,
    THashMap<TString, TStorageAdapterPtr> devices);

}   // namespace NCloud::NBlockStore::NStorage
