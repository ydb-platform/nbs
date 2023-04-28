#pragma once

#include "public.h"

#include <util/generic/hash_set.h>
#include <util/generic/hash.h>
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
    void Insert(const TString& diskId, TVector<TString> uuids);
    void Insert(const TString& diskId, TString uuid);

    TString EraseDevice(const TString& uuid);
    bool EraseDisk(const TString& diskId);

    [[nodiscard]] TString FindDiskId(const TString& uuid) const;
    [[nodiscard]] bool IsEmpty() const;
};

}   // namespace NCloud::NBlockStore::NStorage
