#include "start_endpoint.h"

#include <cloud/blockstore/libs/encryption/model/utils.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/version/version.h>

#include <library/cpp/protobuf/util/pb_io.h>

#include <util/generic/guid.h>
#include <util/string/join.h>

namespace NCloud::NBlockStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
TVector<TString> GetKeys(TMap<TString, T> map)
{
    TVector<TString> keys(Reserve(map.size()));

    for (const auto& item: map) {
        keys.emplace_back(item.first);
    }
    return keys;
}

////////////////////////////////////////////////////////////////////////////////

TMap<TString, NProto::EClientIpcType> GetIpcTypes()
{
    auto minType = static_cast<int>(NProto::EClientIpcType_MIN);
    auto maxType = static_cast<int>(NProto::EClientIpcType_MAX);

    TMap<TString, NProto::EClientIpcType> ipcTypes;
    for (auto i = minType; i <= maxType; ++i) {
        auto ipcType = static_cast<NProto::EClientIpcType>(i);
        ipcTypes.emplace(GetIpcTypeString(ipcType), ipcType);
    }
    return ipcTypes;
};

TMaybe<NProto::EClientIpcType> GetIpcType(const TString& typeStr)
{
    auto ipcTypes = GetIpcTypes();
    auto it = ipcTypes.find(typeStr);
    if (it != ipcTypes.end()) {
        return it->second;
    }
    return {};
}

////////////////////////////////////////////////////////////////////////////////

static const TMap<TString, NProto::EVolumeAccessMode> AccessModes = {
    { "rw",      NProto::VOLUME_ACCESS_READ_WRITE      },
    { "ro",      NProto::VOLUME_ACCESS_READ_ONLY       },
    { "repair",  NProto::VOLUME_ACCESS_REPAIR          },
    { "user-ro", NProto::VOLUME_ACCESS_USER_READ_ONLY  },
};

TMaybe<NProto::EVolumeAccessMode> GetAccessMode(const TString& modeStr)
{
    auto it = AccessModes.find(modeStr);
    if (it != AccessModes.end()) {
        return it->second;
    }
    return {};
}

////////////////////////////////////////////////////////////////////////////////

static const TMap<TString, NProto::EVolumeMountMode> MountModes = {
    { "local",  NProto::VOLUME_MOUNT_LOCAL  },
    { "remote", NProto::VOLUME_MOUNT_REMOTE },
};

TMaybe<NProto::EVolumeMountMode> GetMountMode(const TString& modeStr)
{
    auto it = MountModes.find(modeStr);
    if (it != MountModes.end()) {
        return it->second;
    }
    return {};
}

////////////////////////////////////////////////////////////////////////////////

class TStartEndpointCommand final
    : public TCommand
{
private:
    TString UnixSocketPath;
    TString DiskId;
    TString IpcTypeStr;
    TString ClientId;
    TString InstanceId;
    TString AccessModeStr;
    TString MountModeStr;
    bool ThrottlingDisabled = false;
    ui64 MountSeqNumber = 0;
    ui32 VhostQueuesCount = 1;
    bool UnalignedRequestsDisabled = false;
    NProto::EEncryptionMode EncryptionMode = NProto::NO_ENCRYPTION;
    TString EncryptionKeyPath;
    TString EncryptionKeyHash;
    bool Persistent = false;
    TString NbdDeviceFile;
    THashSet<TString> CGroups;

public:
    TStartEndpointCommand(IBlockStorePtr client)
        : TCommand(std::move(client))
    {
        Opts.AddLongOption("socket", "unix-socket path")
            .RequiredArgument("STR")
            .StoreResult(&UnixSocketPath);

        Opts.AddLongOption("disk-id", "volume identifier")
            .RequiredArgument("STR")
            .StoreResult(&DiskId);

        auto ipcTypes = JoinSeq('|', GetKeys(GetIpcTypes()));
        Opts.AddLongOption("ipc-type", "IPC type used by client [" + ipcTypes + "]")
            .RequiredArgument("STR")
            .StoreResult(&IpcTypeStr);

        Opts.AddLongOption("client-id", "client identifier")
            .RequiredArgument("STR")
            .StoreResult(&ClientId);

        Opts.AddLongOption("instance-id", "VM information")
            .RequiredArgument("STR")
            .StoreResult(&InstanceId);

        auto accessModes = JoinSeq('|', GetKeys(AccessModes));
        Opts.AddLongOption("access-mode", "volume access mode [" + accessModes + "]")
            .RequiredArgument("STR")
            .StoreResult(&AccessModeStr);

        auto mountModes = JoinSeq('|', GetKeys(MountModes));
        Opts.AddLongOption("mount-mode", "volume mount mode [" + mountModes + "]")
            .RequiredArgument("STR")
            .StoreResult(&MountModeStr);

        Opts.AddLongOption("disable-throttling", "explicitly disable throttling")
            .NoArgument()
            .SetFlag(&ThrottlingDisabled);

        Opts.AddLongOption("seqnumber", "volume generation")
            .RequiredArgument("NUM")
            .DefaultValue(0)
            .StoreResult(&MountSeqNumber);

        Opts.AddLongOption("vhost-queues", "vhost queues count")
            .RequiredArgument("NUM")
            .DefaultValue(1)
            .StoreResult(&VhostQueuesCount);

        Opts.AddLongOption("disable-unaligned-requests", "explicitly disable unaligned request support")
            .NoArgument()
            .SetFlag(&UnalignedRequestsDisabled);

        Opts.AddLongOption("encryption-mode", "encryption mode [no|aes-xts|test]")
            .RequiredArgument("STR")
            .Handler1T<TString>([this] (const auto& s) {
                EncryptionMode = EncryptionModeFromString(s);
            });

        Opts.AddLongOption("encryption-key-path", "path to file with encryption key")
            .RequiredArgument("STR")
            .StoreResult(&EncryptionKeyPath);

        Opts.AddLongOption("encryption-key-hash", "key hash for snapshot mode")
            .RequiredArgument("STR")
            .StoreResult(&EncryptionKeyHash);

        Opts.AddLongOption("persistent", "add endpoint to keyring")
            .NoArgument()
            .SetFlag(&Persistent);

        Opts.AddLongOption("nbd-device", "nbd device file which nbd-client connected to")
            .RequiredArgument("STR")
            .StoreResult(&NbdDeviceFile);

        Opts.AddLongOption("cgroup", "cgroup to place into")
            .RequiredArgument("STR")
            .InsertTo(&CGroups);
    }

protected:
    bool DoExecute() override
    {
        if (!Proto && !CheckOpts()) {
            return false;
        }

        auto& input = GetInputStream();
        auto& output = GetOutputStream();

        STORAGE_DEBUG("Reading StartEndpoint request");
        auto request = std::make_shared<NProto::TStartEndpointRequest>();
        if (Proto) {
            ParseFromTextFormat(input, *request);
        } else {
            if (ClientId.empty()) {
                ClientId = CreateGuidAsString();
                output << "Generated client id: " << ClientId << Endl;
            }
            request->SetUnixSocketPath(UnixSocketPath);
            request->SetDiskId(DiskId);
            request->SetIpcType(*GetIpcType(IpcTypeStr));
            request->SetClientId(ClientId);
            if (InstanceId) {
                request->SetInstanceId(InstanceId);
            }
            if (AccessModeStr) {
                request->SetVolumeAccessMode(*GetAccessMode(AccessModeStr));
            }
            if (MountModeStr) {
                request->SetVolumeMountMode(*GetMountMode(MountModeStr));
            }
            request->SetClientVersionInfo(GetFullVersionString());
            request->SetThrottlingDisabled(ThrottlingDisabled);
            request->SetMountSeqNumber(MountSeqNumber);
            request->SetVhostQueuesCount(VhostQueuesCount);
            request->SetUnalignedRequestsDisabled(UnalignedRequestsDisabled);
            request->MutableEncryptionSpec()->CopyFrom(
                CreateEncryptionSpec(
                    EncryptionMode,
                    EncryptionKeyPath,
                    EncryptionKeyHash));
            request->SetPersistent(Persistent);
            request->SetNbdDeviceFile(NbdDeviceFile);
            request->MutableClientCGroups()->Assign(CGroups.begin(), CGroups.end());
        }

        STORAGE_DEBUG("Sending StartEndpoint request");
        const auto requestId = GetRequestId(*request);
        auto result = WaitFor(ClientEndpoint->StartEndpoint(
            MakeIntrusive<TCallContext>(requestId),
            std::move(request)));

        STORAGE_DEBUG("Received StartEndpoint response");
        if (Proto) {
            SerializeToTextFormat(result, output);
            return true;
        }

        if (HasError(result)) {
            output << FormatError(result.GetError()) << Endl;
            return false;
        }

        output << "OK" << Endl;
        return true;
    }

private:
    bool CheckOpts() const
    {
        if (!UnixSocketPath) {
            STORAGE_ERROR("Unix socket path is required");
            return false;
        }

        if (!DiskId) {
            STORAGE_ERROR("Disk id is required");
            return false;
        }

        if (!IpcTypeStr) {
            STORAGE_ERROR("Ipc type is required");
            return false;
        }

        if (!GetIpcType(IpcTypeStr)) {
            STORAGE_ERROR("Unknown ipc type: " << IpcTypeStr.Quote());
            return false;
        }

        if (AccessModeStr && !GetAccessMode(AccessModeStr)) {
            STORAGE_ERROR("Unknown access mode: " << AccessModeStr.Quote());
            return false;
        }

        if (MountModeStr && !GetMountMode(MountModeStr)) {
            STORAGE_ERROR("Unknown mount mode: " << MountModeStr.Quote());
            return false;
        }

        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewStartEndpointCommand(IBlockStorePtr client)
{
    return MakeIntrusive<TStartEndpointCommand>(std::move(client));
}

}   // namespace NCloud::NBlockStore::NClient
