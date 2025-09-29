#pragma once

#include "private.h"

#include <util/datetime/base.h>
#include <util/generic/size_literals.h>
#include <util/generic/string.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

enum class ETestMode
{
    Target,
    Initiator
};

////////////////////////////////////////////////////////////////////////////////

enum class EWaitMode
{
    Poll,
    BusyWait,
    AdaptiveWait,
};

////////////////////////////////////////////////////////////////////////////////

enum class EStorageKind
{
    Null,
    Memory,
    LocalAIO,
    LocalURing,
    Rdma,
};

////////////////////////////////////////////////////////////////////////////////

struct TOptions
{
    ETestMode TestMode = ETestMode::Target;
    TString VerboseLevel;

    // connection options
    TString Host = "localhost";
    ui32 Port = 1234;
    ui32 Backlog = 10;
    ui32 QueueSize = 256;
    ui32 PollerThreads = 1;
    EWaitMode WaitMode = EWaitMode::Poll;
    ui32 ConnectTimeout = 5;
    ui8 Tos = 0;
    TString SourceInterface;

    // storage options
    EStorageKind StorageKind = EStorageKind::Null;
    TString StoragePath;
    ui32 BlockSize = 4_KB;
    ui32 BlocksCount = 1024*1024;

    // test options
    ui32 MaxIoDepth = 1;
    ui32 MinBlocksCount = 1;
    ui32 MaxBlocksCount = 1;
    ui32 WriteRate = 50;
    TDuration TestDuration;

    ui32 ThreadPool = 0;

    // request tracing
    TString TracePath;
    ui32 TraceRate = 1;

    void Parse(int argc, char** argv);
};

}   // namespace NCloud::NBlockStore
