#include "options.h"

#include <cloud/blockstore/libs/encryption/model/utils.h>

#include <library/cpp/getopt/small/last_getopt.h>

#include <util/generic/map.h>

namespace NCloud::NBlockStore::NBD {

using namespace NLastGetopt;

namespace {

////////////////////////////////////////////////////////////////////////////////

static const TMap<TString, EDeviceMode> DeviceModes = {
    { "endpoint",   EDeviceMode::Endpoint },
    { "proxy",      EDeviceMode::Proxy    },
    { "null",       EDeviceMode::Null     },
};

EDeviceMode DeviceModeFromString(const TString& s)
{
    auto it = DeviceModes.find(s);
    if (it != DeviceModes.end()) {
        return it->second;
    }

    ythrow yexception() << "invalid device mode: " << s;
}

////////////////////////////////////////////////////////////////////////////////

static const TMap<TString, NProto::EVolumeAccessMode> AccessModes = {
    { "rw",      NProto::VOLUME_ACCESS_READ_WRITE     },
    { "ro",      NProto::VOLUME_ACCESS_READ_ONLY      },
    { "repair",  NProto::VOLUME_ACCESS_REPAIR         },
    { "user-ro", NProto::VOLUME_ACCESS_USER_READ_ONLY },
};

NProto::EVolumeAccessMode AccessModeFromString(const TString& s)
{
    auto it = AccessModes.find(s);
    if (it != AccessModes.end()) {
        return it->second;
    }

    ythrow yexception() << "invalid access mode: " << s;
}

////////////////////////////////////////////////////////////////////////////////

static const TMap<TString, NProto::EVolumeMountMode> MountModes = {
    { "local",  NProto::VOLUME_MOUNT_LOCAL  },
    { "remote", NProto::VOLUME_MOUNT_REMOTE },
};

NProto::EVolumeMountMode MountModeFromString(const TString& s)
{
    auto it = MountModes.find(s);
    if (it != MountModes.end()) {
        return it->second;
    }

    ythrow yexception() << "invalid mount mode: " << s;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TOptions::Parse(int argc, char** argv)
{
    TOpts opts;
    opts.AddHelpOption();

    opts.AddLongOption("config", "config file name")
        .RequiredArgument("STR")
        .StoreResult(&ConfigFile);

    opts.AddLongOption("host", "connect host")
        .RequiredArgument("STR")
        .StoreResult(&Host);

    opts.AddLongOption("port", "connect port")
        .RequiredArgument("NUM")
        .StoreResult(&InsecurePort);

    opts.AddLongOption("secure-port", "connect secure port (overrides --port)")
        .RequiredArgument("NUM")
        .StoreResult(&SecurePort);

    opts.AddLongOption("iam-token-file", "path to iam token")
        .RequiredArgument("STR")
        .StoreResult(&IamTokenFile);

    opts.AddLongOption("device-mode", "nbs device connection mode [endpoint, proxy, null]")
        .RequiredArgument("STR")
        .Handler1T<TString>([this] (const auto& s) {
            DeviceMode = DeviceModeFromString(s);
        });

    opts.AddLongOption("mon-file")
        .RequiredArgument("STR")
        .StoreResult(&MonitoringConfig);

    opts.AddLongOption("mon-address")
        .RequiredArgument("STR")
        .StoreResult(&MonitoringAddress);

    opts.AddLongOption("mon-port")
        .RequiredArgument("NUM")
        .StoreResult(&MonitoringPort);

    opts.AddLongOption("mon-threads")
        .RequiredArgument("NUM")
        .StoreResult(&MonitoringThreads);

    opts.AddLongOption("disk-id", "volume identifier")
        .RequiredArgument("STR")
        .StoreResult(&DiskId);

    opts.AddLongOption("token", "mount token")
        .RequiredArgument("STR")
        .StoreResult(&MountToken);

    opts.AddLongOption("checkpoint-id", "checkpoint identifier")
        .RequiredArgument("STR")
        .StoreResult(&CheckpointId);

    opts.AddLongOption("access-mode", "volume access mode [rw|ro|repair|user-ro]")
        .RequiredArgument("STR")
        .Handler1T<TString>([this] (const auto& s) {
            AccessMode = AccessModeFromString(s);
        });

    opts.AddLongOption("mount-mode", "volume mount mode [local|remote]")
        .RequiredArgument("STR")
        .Handler1T<TString>([this] (const auto& s) {
            MountMode = MountModeFromString(s);
        });

    opts.AddLongOption("encryption-mode", "encryption mode [no|aes-xts|test]")
        .RequiredArgument("STR")
        .Handler1T<TString>([this] (const auto& s) {
            EncryptionMode = EncryptionModeFromString(s);
        });

    opts.AddLongOption("encryption-key-path", "path to file with encryption key")
        .RequiredArgument("STR")
        .StoreResult(&EncryptionKeyPath);

    opts.AddLongOption("throttling-disabled", "sets MF_THROTTLING_DISABLED mount flag")
        .NoArgument()
        .SetFlag(&ThrottlingDisabled);

    opts.AddLongOption("mount-flags")
        .RequiredArgument("FLAGS")
        .StoreResult(&MountFlags);

    opts.AddLongOption("disable-unaligned-requests")
        .NoArgument()
        .SetFlag(&UnalignedRequestsDisabled);

    opts.AddLongOption("listen-port")
        .RequiredArgument("NUM")
        .StoreResult(&ListenPort);

    opts.AddLongOption("listen-address")
        .RequiredArgument("STR")
        .StoreResult(&ListenAddress);

    opts.AddLongOption("listen-path")
        .RequiredArgument("STR")
        .StoreResult(&ListenUnixSocketPath);

    const auto& device = opts.AddLongOption("connect-device")
        .OptionalArgument("STR")
        .StoreResult(&ConnectDevicePath);

    opts.AddLongOption("null-blocksize")
        .RequiredArgument("NUM")
        .StoreResult(&NullBlockSize);

    opts.AddLongOption("null-blocks-count")
        .RequiredArgument("NUM")
        .StoreResult(&NullBlocksCount);

    opts.AddLongOption("max-inflight-bytes")
        .RequiredArgument("NUM")
        .StoreResult(&MaxInFlightBytes);

    opts.AddLongOption("request-timeout", "request timeout")
        .OptionalArgument("NUM")
        .Handler1T<TString>([this] (const auto& s) {
            RequestTimeout = TDuration::Parse(s);
            Y_ENSURE(
                RequestTimeout.MicroSeconds() % 1000000 == 0,
                "timeout should be a multiple of a second"
            );
        });

    opts.AddLongOption("connection-timeout", "connection timeout")
        .OptionalArgument("NUM")
        .Handler1T<TString>([this] (const auto& s) {
            ConnectionTimeout = TDuration::Parse(s);
            Y_ENSURE(
                ConnectionTimeout.MicroSeconds() % 1000000 == 0,
                "connection timeout should be a multiple of a second"
            );
        });

    const auto& verbose = opts.AddLongOption("verbose", "output level for diagnostics messages")
        .OptionalArgument("STR")
        .StoreResult(&VerboseLevel);

    opts.AddLongOption("grpc-trace", "turn on grpc tracing")
        .NoArgument()
        .StoreTrue(&EnableGrpcTracing);

    opts.AddLongOption("netlink", "use netlink interface to set up device")
        .NoArgument()
        .SetFlag(&Netlink);

    TOptsParseResultException res(&opts, argc, argv);

    if (res.Has(&verbose) && !VerboseLevel) {
        VerboseLevel = "debug";
    }

    Y_ENSURE(DeviceMode == EDeviceMode::Null || DiskId);

    if (DeviceMode == EDeviceMode::Endpoint) {
        Y_ENSURE(ListenUnixSocketPath,
            "'--listen-path' option is required for endpoint device-mode");
    }

    if (res.Has(&device)) {
        ConnectDevice = true;
    }
}

}   // namespace NCloud::NBlockStore::NBD
