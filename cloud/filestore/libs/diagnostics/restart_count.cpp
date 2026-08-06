#include "restart_count.h"

#include <util/generic/yexception.h>
#include <util/stream/file.h>
#include <util/string/cast.h>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

void PublishRestartsCount(
    const TString& restartsCountFile,
    NMonitoring::TDynamicCountersPtr rootCounters)
{
    if (restartsCountFile.empty()) {
        return;
    }

    auto counter = rootCounters->GetSubgroup("counters", "utils")
                       ->GetCounter("RestartsCount", false);
    ui32 restartsCount = 0;

    // Intentionally mirrors YDB's TRestartsCountPublisher in
    // contrib/ydb/core/driver_lib/run/kikimr_services_initializers.cpp.
    try {
        TUnbufferedFileInput fileInput(restartsCountFile);
        restartsCount = FromString<ui32>(fileInput.ReadAll());
    } catch (const yexception&) {
        restartsCount = 0;
    }

    *counter = restartsCount;

    TUnbufferedFileOutput fileOutput(restartsCountFile);
    fileOutput.Write(ToString(restartsCount + 1));
}

}   // namespace NCloud::NFileStore
