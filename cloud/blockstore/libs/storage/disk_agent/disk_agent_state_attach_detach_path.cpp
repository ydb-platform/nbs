#include "disk_agent_state.h"

#include <cloud/blockstore/libs/storage/disk_agent/model/config.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/device_scanner.h>

#include <regex>

namespace NCloud::NBlockStore::NStorage {

TDiskAgentConfigPtr TDiskAgentState::CreateConfigForAttachValidation(
    const TVector<TString>& paths,
    const TVector<NProto::TDeviceConfig>& devices)
{
    auto protoConfig = AgentConfig->GetProtoConfig();
    protoConfig.ClearFileDevices();
    protoConfig.ClearMemoryDevices();
    protoConfig.ClearNvmeDevices();

    THashMap<TString, bool> hasLayout;
    for (const auto& path: paths) {
        ui64 fileLength = 0;
        try {
            fileLength = GetFileLengthWithSeek(path);
        } catch (const std::exception& e) {
            return {};
        }

        for (const auto& c:
             AgentConfig->GetStorageDiscoveryConfig().GetPathConfigs())
        {
            std::regex regex(c.GetPathRegExp().c_str());
            if (std::regex_match(path.c_str(), regex)) {
                const auto* poolConfig = FindPoolConfig(c, fileLength);
                hasLayout[path] = poolConfig ? poolConfig->HasLayout() : false;
                break;
            }
        }
    }

    for (const auto& device: devices) {
        auto* fileDevice = protoConfig.AddFileDevices();
        fileDevice->SetPath(device.GetDeviceName());
        fileDevice->SetBlockSize(device.GetBlockSize());
        fileDevice->SetDeviceId(device.GetDeviceUUID());
        fileDevice->SetPoolName(device.GetPoolName());

        if (hasLayout[device.GetDeviceName()]) {
            fileDevice->SetOffset(device.GetPhysicalOffset());
            fileDevice->SetFileSize(
                device.GetBlocksCount() * device.GetBlockSize());
        }
        fileDevice->SetSerialNumber(device.GetSerialNumber());
    }

    return std::make_shared<TDiskAgentConfig>(
        protoConfig,
        AgentConfig->GetRack(),
        AgentConfig->GetNetworkMbitThroughput());
}

}   // namespace NCloud::NBlockStore::NStorage
