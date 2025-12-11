#pragma once

#include "public.h"

#include <cloud/blockstore/libs/common/public.h>
#include <cloud/blockstore/libs/service/public.h>
#include <cloud/blockstore/libs/spdk/iface/public.h>
#include <cloud/blockstore/libs/storage/protos/disk.pb.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TInitializeSpdkResult
{
    NSpdk::ISpdkTargetPtr SpdkTarget;
    TVector<NProto::TDeviceConfig> Configs;
    TVector<IStoragePtr> Devices;
    TVector<TString> Errors;
};

NThreading::TFuture<TInitializeSpdkResult> InitializeSpdk(
    TDiskAgentConfigPtr agentConfig,
    NSpdk::ISpdkEnvPtr spdk,
    ICachingAllocatorPtr allocator);

}   // namespace NCloud::NBlockStore::NStorage
