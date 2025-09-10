#pragma once

#include "public.h"

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/stream/output.h>

namespace NCloud::NBlockStore::NLoadTest {

////////////////////////////////////////////////////////////////////////////////

struct TOptions
{
    TString TestName;
    TString ClientConfig;
    TString Host;
    ui32 InsecurePort = 0;
    ui32 SecurePort = 0;

    TString MonitoringConfig;
    TString MonitoringAddress;
    ui32 MonitoringPort = 0;
    ui32 MonitoringThreads = 0;

    TString SpdkConfig;
    TString TestConfig;

    TString OutputFile;
    std::unique_ptr<IOutputStream> OutputStream;

    TString EndpointStorageDir;

    TDuration Timeout = TDuration::Zero();

    TString VerboseLevel;
    bool EnableGrpcTracing = false;

    void Parse(int argc, char** argv);

    IOutputStream& GetOutputStream();
};

}   // namespace NCloud::NBlockStore::NLoadTest
