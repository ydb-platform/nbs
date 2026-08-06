#pragma once

#include <cloud/storage/core/libs/diagnostics/public.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/string.h>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

void PublishRestartsCount(
    const TString& restartsCountFile,
    NMonitoring::TDynamicCountersPtr rootCounters);

}   // namespace NCloud::NFileStore
