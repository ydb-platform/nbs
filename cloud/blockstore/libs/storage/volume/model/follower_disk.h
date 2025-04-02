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

    TString Uuid;
    TString FollowerDiskId;
    TString ScaleUnitId;
    EState State = EState::None;
    std::optional<ui64> MigrationBlockIndex;

    TString GetDiskIdForPrint() const;
    bool operator==(const TFollowerDiskInfo& rhs) const;
};

using TFollowerDisks = TVector<TFollowerDiskInfo>;

}   // namespace NCloud::NBlockStore::NStorage
