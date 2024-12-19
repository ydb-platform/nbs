#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/error.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TPendingCleanup
{
public:
    using TAllocatingDiskId = TString;
    using TDeallocatingDiskId = TString;

    // both values are optional with empty string being empty value
    struct TOpt2Disk
    {
        TAllocatingDiskId AllocatingDiskId;
        TDeallocatingDiskId DeallocatingDiskId;
    };

private:
    THashMap<TString, int> DiskToDeviceCount;
    THashMap<TString, TOpt2Disk> DeviceToDisk;

public:
    [[nodiscard]] NProto::TError Insert(
        const TString& diskId,
        TVector<TString> uuids,
        bool allocation = false);
    [[nodiscard]] NProto::TError
    Insert(const TString& diskId, TString uuid, bool allocation = false);

    /// Removes the device from deallocating disk (for pending deallocation
    /// disks)
    /// @return diskId of allocated/deallocated disk if the device with the
    /// given UUID was the last to complete its allocation/deallocation
    TOpt2Disk EraseDevice(const TString& uuid);
    bool EraseDisk(const TString& diskId);

    [[nodiscard]] TOpt2Disk FindDiskId(const TString& uuid) const;
    [[nodiscard]] bool IsEmpty() const;
    [[nodiscard]] bool Contains(const TString& diskId) const;

    /// If device belongs to pending allocation disk, forget this disk
    /// @return diskId of allocating disk if existed, empty string otherwise
    [[nodiscard]] TAllocatingDiskId CancelPendingAllocation(const TString& uuid);

private:
    [[nodiscard]] NProto::TError ValidateInsertion(
        const TString& diskId,
        const TVector<TString>& uuids,
        bool allocation) const;
};

}   // namespace NCloud::NBlockStore::NStorage
