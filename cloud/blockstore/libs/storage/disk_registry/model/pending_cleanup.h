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
private:
    THashMap<TString, int> DiskToDeviceCount;
    THashMap<TString, TString> DeviceToDisk;

public:
    [[nodiscard]] NProto::TError Insert(
        const TString& diskId,
        TVector<TString> uuids);
    [[nodiscard]] NProto::TError Insert(const TString& diskId, TString uuid);

    TString EraseDevice(const TString& uuid);
    bool EraseDisk(const TString& diskId);

    [[nodiscard]] TString FindDiskId(const TString& uuid) const;
    [[nodiscard]] bool IsEmpty() const;
    [[nodiscard]] bool Contains(const TString& diskId) const;

private:
    [[nodiscard]] NProto::TError ValidateInsertion(
        const TString& diskId,
        const TVector<TString>& uuids) const;
};

}   // namespace NCloud::NBlockStore::NStorage
