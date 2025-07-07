#pragma once

#include <cloud/blockstore/public/api/protos/encryption.pb.h>
#include <cloud/storage/core/libs/common/affinity.h>

#include <util/datetime/base.h>
#include <util/generic/size_literals.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/sysstat.h>

namespace NCloud::NBlockStore::NVHostServer {

////////////////////////////////////////////////////////////////////////////////

struct TDeviceChunk
{
    TString DevicePath;
    ui64 ByteCount = 0;
    ui64 Offset = 0;
};

struct TOptions
{
    TString SocketPath;
    TString DiskId;
    TString Serial;
    TString DeviceBackend = "aio";
    TVector<TDeviceChunk> Layout;
    bool ReadOnly = false;
    bool NoSync = false;
    bool NoChmod = false;
    ui32 BatchSize = 1024;
    ui32 BlockSize = 512;
    ui32 QueueCount = 0;
    ui64 PteFlushByteThreshold = 0;
    ui32 SocketAccessMode = S_IRGRP | S_IWGRP | S_IRUSR | S_IWUSR;

    TString LogType = "json";
    TString VerboseLevel = "info";

    TString ClientId = "vhost-server";
    TDuration WaitAfterParentExit = TDuration::Seconds(60);

    NProto::EEncryptionMode EncryptionMode = NProto::NO_ENCRYPTION;
    TString EncryptionKeyPath;
    ui32 EncryptionKeyringId = 0;
    __pid_t BlockstoreServicePid = 0;

    NProto::TEncryptionSpec GetEncryptionSpec() const;

    struct
    {
        ui32 QueueSize = 256;
        ui32 MaxBufferSize = 4_MB + 4_KB;
        bool AlignedData = false;
    } RdmaClient;

    void Parse(int argc, char** argv);
};

}   // namespace NCloud::NBlockStore::NVHostServer
