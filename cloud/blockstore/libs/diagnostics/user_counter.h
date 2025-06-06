#pragma once

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/monlib/metrics/metric_registry.h>

#include <cloud/storage/core/libs/user_stats/counter/user_counter.h>

#include <util/generic/hash_multi_map.h>

namespace NCloud::NBlockStore::NUserCounter {

using IUserCounterSupplier = NCloud::NStorage::NUserStats::IUserCounterSupplier;

////////////////////////////////////////////////////////////////////////////////

void RegisterServiceVolume(
    IUserCounterSupplier& dsc,
    const TString& cloudId,
    const TString& folderId,
    const TString& diskId,
    NMonitoring::TDynamicCounterPtr src);

void UnregisterServiceVolume(
    IUserCounterSupplier& dsc,
    const TString& cloudId,
    const TString& folderId,
    const TString& diskId);

void RegisterServerVolumeInstance(
    IUserCounterSupplier& dsc,
    const TString& cloudId,
    const TString& folderId,
    const TString& diskId,
    const TString& instanceId,
    const bool reportZeroBlocksMetrics,
    NMonitoring::TDynamicCounterPtr src);

void UnregisterServerVolumeInstance(
    IUserCounterSupplier& dsc,
    const TString& cloudId,
    const TString& folderId,
    const TString& diskId,
    const TString& instanceId);

} // NCloud::NBlockStore::NUserCounter
