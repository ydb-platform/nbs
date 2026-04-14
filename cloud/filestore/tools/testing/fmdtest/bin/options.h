#pragma once

#include <util/datetime/base.h>
#include <util/generic/string.h>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

struct TOptions
{
    TString TestDir;

    ui32 ProducerThreads{};
    ui32 FilesPerProducer{};
    ui32 UnlinkPercentage{};
    TDuration ProducerSleepDuration;
    ui32 FileSize{};

    ui32 StealerThreads{};
    TDuration StealerSleepDuration;

    ui32 ListerThreads{};
    TDuration ListerSleepDuration;

    TDuration TestDuration;
    TString ReportPath;

    void Parse(int argc, char** argv);
};

}   // namespace NCloud::NFileStore
