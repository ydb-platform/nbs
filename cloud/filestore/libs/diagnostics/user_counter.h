#pragma once

#include <cloud/storage/core/libs/diagnostics/histogram_counter_options.h>
#include <cloud/storage/core/libs/user_stats/counter/user_counter.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NCloud::NFileStore::NUserCounter {

using IUserCounterSupplier = NCloud::NStorage::NUserStats::IUserCounterSupplier;

////////////////////////////////////////////////////////////////////////////////

void RegisterFilestore(
    IUserCounterSupplier& dsc,
    const TString& cloudId,
    const TString& folderId,
    const TString& filestoreId,
    const TString& instanceId,
    EHistogramCounterOptions histogramCounterOptions,
    NMonitoring::TDynamicCounterPtr src);

void UnregisterFilestore(
    IUserCounterSupplier& dsc,
    const TString& cloudId,
    const TString& folderId,
    const TString& filestoreId,
    const TString& instanceId);

}   // NCloud::NFileStore::NUserCounter
