#pragma once

#include "private.h"

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/stream/output.h>

namespace NCloud::NFileStore::NLoadTest {

////////////////////////////////////////////////////////////////////////////////

struct TOptions
{
    TString ConfigFile;
    TString Host;
    ui32 InsecurePort = 0;
    ui32 SecurePort = 0;
    TString UnixSocketPath;

    TString MonitoringConfig;
    TString MonitoringAddress;
    ui32 MonitoringPort = 0;
    ui32 MonitoringThreads = 0;

    TString TestsConfig;

    TString OutputFile;
    std::unique_ptr<IOutputStream> OutputStream;

    TDuration Timeout = TDuration::Zero();

    TString VerboseLevel;
    bool EnableGrpcTracing = false;

    void Parse(int argc, char** argv);

    IOutputStream& GetOutputStream();
};

}   // namespace NCloud::NFileStore::NLoadTest
