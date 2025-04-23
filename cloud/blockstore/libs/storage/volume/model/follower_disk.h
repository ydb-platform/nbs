#pragma once

#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <optional>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TFollowerDiskInfo
{
    enum class EState
    {
        None = 0,
        Preparing = 1,
        Ready = 2,
        Error = 3,
    };

    TString LinkUUID;
    TString FollowerDiskId;
    TString ScaleUnitId;
    EState State = EState::None;
    std::optional<ui64> MigratedBytes;

    TString GetDiskIdForPrint() const;
    TString Describe() const;
    bool operator==(const TFollowerDiskInfo& rhs) const;
};

using TFollowerDisks = TVector<TFollowerDiskInfo>;

}   // namespace NCloud::NBlockStore::NStorage
