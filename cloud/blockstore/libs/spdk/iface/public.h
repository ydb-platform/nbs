#pragma once

#include <util/generic/guid.h>

#include <memory>

namespace NCloud::NBlockStore::NSpdk {

////////////////////////////////////////////////////////////////////////////////

struct TDeviceStats
{
    TGUID DeviceUUID;
    ui32 BlockSize;
    ui64 BlocksCount;
};

////////////////////////////////////////////////////////////////////////////////

struct TDeviceIoStats
{
    ui64 BytesRead;
    ui64 NumReadOps;
    ui64 BytesWritten;
    ui64 NumWriteOps;
};

////////////////////////////////////////////////////////////////////////////////

struct TDeviceRateLimits
{
    ui64 IopsLimit;
    ui64 BandwidthLimit;
    ui64 ReadBandwidthLimit;
    ui64 WriteBandwidthLimit;
};

////////////////////////////////////////////////////////////////////////////////

class TSpdkEnvConfig;
using TSpdkEnvConfigPtr = std::shared_ptr<TSpdkEnvConfig>;

struct ISpdkEnv;
using ISpdkEnvPtr = std::shared_ptr<ISpdkEnv>;

struct ISpdkDevice;
using ISpdkDevicePtr = std::shared_ptr<ISpdkDevice>;

struct ISpdkTarget;
using ISpdkTargetPtr = std::shared_ptr<ISpdkTarget>;

using TSpdkBuffer = std::shared_ptr<char>;

}   // namespace NCloud::NBlockStore::NSpdk
