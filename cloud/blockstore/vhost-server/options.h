#pragma once

#include <cloud/storage/core/libs/common/affinity.h>

#include <util/generic/size_literals.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NVHostServer {

////////////////////////////////////////////////////////////////////////////////

struct TDeviceChunk
{
    TString DevicePath;
    i64 ByteCount = 0;
    i64 Offset = 0;
};

struct TOptions
{
    TString SocketPath;
    TString Serial;
    TString DeviceBackend = "aio";
    TVector<TDeviceChunk> Layout;
    bool ReadOnly = false;
    bool NoSync = false;
    bool NoChmod = false;
    ui32 BatchSize = 1024;
    ui32 QueueCount = 0;

    TString LogType = "json";
    TString VerboseLevel = "info";

    void Parse(int argc, char** argv);
};

}   // namespace NCloud::NBlockStore::NVHostServer
