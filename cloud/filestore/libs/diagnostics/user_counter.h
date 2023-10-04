#pragma once

#include <cloud/storage/core/libs/user_stats/counter/user_counter.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NCloud::NFileStore::NUserCounter {

////////////////////////////////////////////////////////////////////////////////

void RegisterFilestore(
    NCloud::NStorage::NUserStats::TUserCounterSupplier& dsc,
    const TString& cloudId,
    const TString& folderId,
    const TString& filestoreId,
    const TString& instanceId,
    NMonitoring::TDynamicCounterPtr src);

void UnregisterFilestore(
    NCloud::NStorage::NUserStats::TUserCounterSupplier& dsc,
    const TString& cloudId,
    const TString& folderId,
    const TString& filestoreId,
    const TString& instanceId);

}   // NCloud::NFileStore::NUserCounter
