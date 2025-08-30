#pragma once

#include "public.h"

#include <util/generic/hash_set.h>
#include <util/generic/string.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TDeviceReplacementTracker
{
private:
    THashSet<TString> DeviceReplacementIds;

public:
    bool AddDeviceReplacement(const TString& id);
    bool RemoveDeviceReplacement(const TString& id);

    [[nodiscard]] bool HasDeviceReplacement(const TString& id) const;
    [[nodiscard]] const THashSet<TString>& GetDevicesReplacements() const;
};

}   // namespace NCloud::NBlockStore::NStorage
