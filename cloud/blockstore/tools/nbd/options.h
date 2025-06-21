#pragma once

#include "private.h"

#include <cloud/blockstore/public/api/protos/volume.pb.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>

namespace NCloud::NBlockStore::NBD {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 NBD_DEFAULT_PORT = 10809;

////////////////////////////////////////////////////////////////////////////////

enum class EDeviceMode
{
    Endpoint,
    Proxy,
    Null,
};

////////////////////////////////////////////////////////////////////////////////

struct TOptions
{
    TString ConfigFile;
    TString Host;
    ui32 InsecurePort = 0;
    ui32 SecurePort = 0;
    TString IamTokenFile;

    EDeviceMode DeviceMode = EDeviceMode::Endpoint;

    TString MonitoringConfig;
    TString MonitoringAddress;
    ui32 MonitoringPort = 0;
    ui32 MonitoringThreads = 0;

    TString DiskId;
    TString MountToken;
    TString CheckpointId;
    NProto::EEncryptionMode EncryptionMode = NProto::NO_ENCRYPTION;
    TString EncryptionKeyPath;
    NProto::EVolumeAccessMode AccessMode = NProto::VOLUME_ACCESS_READ_ONLY;
    NProto::EVolumeMountMode MountMode = NProto::VOLUME_MOUNT_REMOTE;

    ui32 ListenPort = NBD_DEFAULT_PORT;
    TString ListenAddress;
    TString ListenUnixSocketPath;

    bool ConnectDevice = false;
    TString ConnectDevicePath;

    bool Netlink = false;
    bool Disconnect = false;
    bool Reconfigure = false;

    ui32 NullBlockSize = 4*1024;
    ui64 NullBlocksCount = 1024*1024;

    ui64 MaxInFlightBytes = 128*1024*1024;

    TString VerboseLevel;
    bool EnableGrpcTracing = false;

    bool ThrottlingDisabled = false;
    ui32 MountFlags = 0;

    bool UnalignedRequestsDisabled = false;

    TDuration RequestTimeout = TDuration::Minutes(5);
    TDuration ConnectionTimeout = TDuration::Hours(1);

    void Parse(int argc, char** argv);
};

}   // namespace NCloud::NBlockStore::NBD
