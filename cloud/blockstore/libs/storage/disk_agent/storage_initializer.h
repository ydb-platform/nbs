#pragma once

#include "public.h"

#include "storage_with_stats.h"

#include <cloud/blockstore/libs/common/public.h>
#include <cloud/blockstore/libs/nvme/public.h>
#include <cloud/blockstore/libs/service/public.h>
#include <cloud/blockstore/libs/storage/core/public.h>
#include <cloud/blockstore/libs/storage/protos/disk.pb.h>

#include <cloud/blockstore/libs/storage/disk_agent/model/device_guard.h>

#include <util/generic/vector.h>

#include <library/cpp/threading/future/future.h>

class TLog;

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TInitializeStorageResult
{
    TVector<NProto::TDeviceConfig> Configs;
    TVector<IStoragePtr> Devices;
    TVector<TStorageIoStatsPtr> Stats;
    TVector<TString> Errors;
    TVector<TString> ConfigMismatchErrors;
    TVector<TString> DevicesWithSuspendedIO;
    TVector<TString> LostDevicesIds;
    TDeviceGuard Guard;
};

NThreading::TFuture<TInitializeStorageResult> InitializeStorage(
    TLog log,
    TStorageConfigPtr storageConfig,
    TDiskAgentConfigPtr agentConfig,
    IStorageProviderPtr storageProvider,
    NNvme::INvmeManagerPtr nvmeManager);

}   // namespace NCloud::NBlockStore::NStorage
