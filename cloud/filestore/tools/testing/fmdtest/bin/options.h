#pragma once

#include <util/generic/string.h>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

struct TOptions
{
    TString TestDir;
    ui32 ProducerThreads = 4;
    ui32 StealerThreads = 2;
    ui32 FilesPerProducer = 1000;
    ui32 TestDurationSec = 60;
    ui32 FileSize = 4096;
    TString ReportPath;

    // When true, stealers may steal files from producers on other MPI ranks.
    // Requires a shared filesystem visible to all ranks under the same path.
    bool MpiCrossRankStealing = true;

    void Parse(int argc, char** argv);
};

}   // namespace NCloud::NFileStore
