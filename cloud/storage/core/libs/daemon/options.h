#pragma once

#include <library/cpp/getopt/small/last_getopt.h>

#include <util/generic/string.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

struct TOptionsBase
{
    NLastGetopt::TOpts Opts;

    TString MonitoringAddress;
    ui32 MonitoringPort = 0;
    ui32 MonitoringThreads = 0;

    TString DiagnosticsConfig;

    TString ServerConfig;
    ui32 ServerPort = 0;
    ui32 SecureServerPort = 0;

    bool EnableGrpcTracing = false;
    TString ProfileFile;

    TString VerboseLevel;
    bool MemLock = false;

    TOptionsBase();
    virtual ~TOptionsBase() = default;

    virtual void Parse(int argc, char** argv) = 0;
};

}   // namespace NCloud
