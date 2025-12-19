#include "pending_cleanup.h"

#include <util/generic/algorithm.h>
#include <util/string/builder.h>
#include <util/string/vector.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

NProto::TError TPendingCleanup::Insert(
    const TString& diskId,
    TVector<TString> uuids)
{
    auto error = ValidateInsertion(diskId, uuids);
    if (HasError(error)) {
        return error;
    }

    DiskToDeviceCount[diskId] += static_cast<int>(uuids.size());

    for (auto& uuid: uuids) {
        Y_DEBUG_ABORT_UNLESS(!uuid.empty());
        auto [_, success] = DeviceToDisk.emplace(std::move(uuid), diskId);
        Y_DEBUG_ABORT_UNLESS(success);
    }

    return {};
}

NProto::TError TPendingCleanup::Insert(const TString& diskId, TString uuid)
{
    return Insert(diskId, TVector{std::move(uuid)});
}

[[nodiscard]] NProto::TError TPendingCleanup::ValidateInsertion(
    const TString& diskId,
    const TVector<TString>& uuids) const
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

        const auto* foundDiskId = DeviceToDisk.FindPtr(uuid);
        if (foundDiskId) {
            return MakeError(
                E_ARGUMENT,
                TStringBuilder() << "Could not insert device: " << uuid
                                 << "; diskId: " << diskId
                                 << " to PendingCleanup. It was already "
                                    "inserted with the diskId: "
                                 << *foundDiskId);
        }
    }

    return {};
}

TString TPendingCleanup::EraseDevice(const TString& uuid)
{
    auto it = DeviceToDisk.find(uuid);
    if (it == DeviceToDisk.end()) {
        return {};
    }

    auto diskId = std::move(it->second);
    DeviceToDisk.erase(it);

    Y_DEBUG_ABORT_UNLESS(DiskToDeviceCount.contains(diskId));
    if (--DiskToDeviceCount[diskId] > 0) {
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
