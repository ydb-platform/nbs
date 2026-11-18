#pragma once

#include <util/datetime/base.h>
#include <util/generic/string.h>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

enum class EMode
{
    // Producers, stealers and listers in a single process.
    Bench,

    // One of the instances running in different VMs, synchronized via the
    // hub: modifies its own directory, checks the others' directories.
    MultiClient,

    // The hub that synchronizes the MultiClient instances.
    SyncHub,
};

struct TOptions
{
    EMode Mode = EMode::Bench;

    TString TestDir;

    ui32 ProducerThreads{};
    ui32 FilesPerProducer{};
    ui32 UnlinkPercentage{};
    TDuration ProducerSleepDuration;
    ui32 FileSize{};
    ui32 CreateFileAttemptCount{};

    ui32 StealerThreads{};
    TDuration StealerSleepDuration;

    ui32 ListerThreads{};
    TDuration ListerSleepDuration;

    TDuration TestDuration;
    TString ReportPath;

    bool SkipFileValidation = false;

    //
    // MultiClient settings.
    //

    ui32 InstanceId{};
    ui32 InstanceCount{};
    TString SyncHost;
    ui16 SyncPort{};
    ui32 Rounds{};
    ui32 FilesPerRound{};
    ui32 RenamePercentage{};
    ui32 OverwritePercentage{};
    TDuration CheckDelay;

    //
    // SyncHub settings (SyncPort and InstanceCount are shared).
    //

    TDuration SyncTimeout;

    void Parse(int argc, char** argv);
};

}   // namespace NCloud::NFileStore
