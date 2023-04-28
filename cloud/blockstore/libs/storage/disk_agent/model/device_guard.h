#pragma once

#include <util/generic/hash_multi_map.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/file.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TDeviceGuard final
{
private:
    THashMultiMap<TString, TFileHandle> Storage;

public:
    TDeviceGuard() = default;

    TDeviceGuard(const TDeviceGuard&) = delete;
    TDeviceGuard& operator=(const TDeviceGuard&) = delete;

    TDeviceGuard(TDeviceGuard&&) noexcept = default;
    TDeviceGuard& operator=(TDeviceGuard&&) noexcept = default;

    bool Lock(const TString& path);
    bool Unlock(const TString& path);
};

}   // namespace NCloud::NBlockStore::NStorage
