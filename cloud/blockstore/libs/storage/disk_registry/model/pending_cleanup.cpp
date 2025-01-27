#include "pending_cleanup.h"

#include <util/generic/algorithm.h>
#include <util/string/builder.h>
#include <util/string/vector.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

NProto::TError TPendingCleanup::Insert(
    const TString& diskId,
    TVector<TString> uuids,
    bool allocation)
{
    auto error = ValidateInsertion(diskId, uuids, allocation);
    if (HasError(error)) {
        return error;
    }

    DiskToDeviceCount[diskId] += static_cast<int>(uuids.size());

    for (auto& uuid: uuids) {
        Y_DEBUG_ABORT_UNLESS(!uuid.empty());
        auto& [allocatingDiskId, deallocatingDiskId] = DeviceToDisk[uuid];
        (allocation ? allocatingDiskId : deallocatingDiskId) = diskId;
    }

    return {};
}

NProto::TError TPendingCleanup::Insert(const TString& diskId, TString uuid, bool allocation)
{
    return Insert(diskId, TVector{std::move(uuid)}, allocation);
}

[[nodiscard]] NProto::TError TPendingCleanup::ValidateInsertion(
    const TString& diskId,
    const TVector<TString>& uuids,
    bool allocation) const
{
    if (diskId.empty() || uuids.empty()) {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder()
                << "Invalid arguments. DiskId: " << diskId << "; deviceIds: ["
                << JoinStrings(uuids, ", ") << "]");
    }

    for (const auto& uuid: uuids) {
        if (uuid.empty()) {
            return MakeError(
                E_ARGUMENT,
                TStringBuilder() << "Invalid arguments. One of the devices is "
                                    "empty. DiskId: "
                                 << diskId << "; deviceIds: ["
                                 << JoinStrings(uuids, ", ") << "]");
        }

        const auto* foundDisks = DeviceToDisk.FindPtr(uuid);
        if (!foundDisks) {
            continue;
        }
        const auto& foundDiskId = allocation ? foundDisks->AllocatingDiskId
                                             : foundDisks->DeallocatingDiskId;
        if (foundDiskId) {
            return MakeError(
                E_ARGUMENT,
                TStringBuilder() << "Could not insert device: " << uuid
                                 << "; diskId: " << diskId
                                 << " to PendingCleanup. It was already "
                                    "inserted with the diskId: "
                                 << foundDiskId);
        }
    }

    return {};
}

TPendingCleanup::TOpt2Disk TPendingCleanup::EraseDevice(const TString& uuid)
{
    auto it = DeviceToDisk.find(uuid);
    if (it == DeviceToDisk.end()) {
        return {};
    }

    auto [allocatingDiskId, deallocatingDiskId] = std::move(it->second);
    DeviceToDisk.erase(it);
    TOpt2Disk ret;

    Y_DEBUG_ABORT_UNLESS(allocatingDiskId || deallocatingDiskId);
    Y_DEBUG_ABORT_UNLESS(!allocatingDiskId || DiskToDeviceCount.contains(allocatingDiskId));
    Y_DEBUG_ABORT_UNLESS(!deallocatingDiskId || DiskToDeviceCount.contains(deallocatingDiskId));
    if (allocatingDiskId && --DiskToDeviceCount[allocatingDiskId] <= 0) {
        DiskToDeviceCount.erase(allocatingDiskId);
        ret.AllocatingDiskId = std::move(allocatingDiskId);
    }

    if (deallocatingDiskId && --DiskToDeviceCount[deallocatingDiskId] <= 0) {
        DiskToDeviceCount.erase(deallocatingDiskId);
        ret.DeallocatingDiskId = std::move(deallocatingDiskId);
    }

    return ret;
}

TPendingCleanup::TOpt2Disk TPendingCleanup::FindDiskId(const TString& uuid) const
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

    for (auto& [device, opt2Disk] : DeviceToDisk) {
        auto& [allocatingDiskId, deallocatingDiskId] = opt2Disk;
        if (allocatingDiskId == diskId) {
            allocatingDiskId.clear();
        }
        if (deallocatingDiskId == diskId) {
            deallocatingDiskId.clear();
        }
    }

    EraseNodesIf(DeviceToDisk, [&] (auto& x) {
        auto& [allocating, deallocating] = x.second;
        return !allocating && !deallocating;
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

TPendingCleanup::TAllocatingDiskId TPendingCleanup::CancelPendingAllocation(const TString& uuid) {
    const auto [allocating, deallocating] = FindDiskId(uuid);
    Y_UNUSED(deallocating);
    if (allocating) {
        EraseDisk(allocating);
    }
    return allocating;
}

}   // namespace NCloud::NBlockStore::NStorage
