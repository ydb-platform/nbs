#pragma once

#include <cloud/blockstore/config/disk.pb.h>
#include <cloud/blockstore/libs/nvme/public.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/public.h>
#include <cloud/blockstore/libs/storage/protos/disk.pb.h>

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/logger/log.h>

#include <util/generic/fwd.h>

#include <memory>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TNVMeDeviceList
{
private:
    const NNvme::INvmeManagerPtr NVMeManager;
    const TDiskAgentConfigPtr Config;
    const TLog Log;

    TVector<NProto::TNVMeDevice> NVMeDevices;

public:
    TNVMeDeviceList(
        NNvme::INvmeManagerPtr nvmeManager,
        TDiskAgentConfigPtr config,
        TLog log);

    ~TNVMeDeviceList();

    [[nodiscard]] const TVector<NProto::TNVMeDevice>& GetNVMeDevices() const;

    [[nodiscard]] NProto::TError BindNVMeDeviceToVFIO(
        const TString& serialNumber);

    // Rebind NVMe device to the nvme driver; crypto erase; recreate single ns
    [[nodiscard]] NProto::TError ResetNVMeDevice(const TString& serialNumber);

private:
    void DiscoverNVMeDevices();
    void UpdateDeviceCache();
};

}   // namespace NCloud::NBlockStore::NStorage
