#pragma once

#include "public.h"

#include "storage_with_stats.h"

#include <cloud/blockstore/libs/common/public.h>
#include <cloud/blockstore/libs/nvme/public.h>
#include <cloud/blockstore/libs/service/public.h>
#include <cloud/blockstore/libs/storage/protos/disk.pb.h>

#include <cloud/blockstore/libs/storage/disk_agent/model/device_guard.h>

#include <util/generic/vector.h>

#include <library/cpp/threading/future/future.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TInitializeStorageResult
{
    TVector<NProto::TDeviceConfig> Configs;
    TVector<IStoragePtr> Devices;
    TVector<TStorageIoStatsPtr> Stats;
    TVector<TString> Errors;
    TDeviceGuard Guard;
};

NThreading::TFuture<TInitializeStorageResult> InitializeStorage(
    TDiskAgentConfigPtr agentConfig,
    IStorageProviderPtr storageProvider,
    NNvme::INvmeManagerPtr nvmeManager);

}   // namespace NCloud::NBlockStore::NStorage
