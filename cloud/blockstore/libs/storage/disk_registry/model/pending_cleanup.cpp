#include "pending_cleanup.h"

#include <util/generic/algorithm.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

void TPendingCleanup::Insert(const TString& diskId, TVector<TString> uuids)
{
    if (uuids.empty()) {
        return;
    }

    DiskToDeviceCount[diskId] += static_cast<int>(uuids.size());

    for (auto& uuid: uuids) {
        DeviceToDisk.emplace(std::move(uuid), diskId);
    }
}

void TPendingCleanup::Insert(const TString& diskId, TString uuid)
{
    if (uuid.empty()) {
        return;
    }

    DiskToDeviceCount[diskId] += 1;
    DeviceToDisk.emplace(std::move(uuid), diskId);
}

TString TPendingCleanup::EraseDevice(const TString& uuid)
{
    auto it = DeviceToDisk.find(uuid);
    if (it == DeviceToDisk.end()) {
        return {};
    }

    auto diskId = std::move(it->second);
    DeviceToDisk.erase(it);

    if (--DiskToDeviceCount[diskId]) {
        return {};
    }

    DiskToDeviceCount.erase(diskId);

    return diskId;
}

TString TPendingCleanup::FindDiskId(const TString& uuid) const
{
    auto it = DeviceToDisk.find(uuid);
    if (it == DeviceToDisk.end()) {
        return {};
    }

    return it->second;
}

bool TPendingCleanup::EraseDisk(const TString& diskId)
{
    if (!DiskToDeviceCount.erase(diskId)) {
        return false;
    }

    EraseNodesIf(DeviceToDisk, [&] (auto& x) {
        return x.second == diskId;
    });

    return true;
}

bool TPendingCleanup::IsEmpty() const
{
    return DiskToDeviceCount.empty();
}

bool TPendingCleanup::Contains(const TString& diskId) const
{
    return DiskToDeviceCount.contains(diskId);
}

}   // namespace NCloud::NBlockStore::NStorage
