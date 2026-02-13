#pragma once

#include <cloud/blockstore/libs/diagnostics/downtime_history.h>

#include <util/generic/hash.h>
#include <util/system/mutex.h>
#include <util/system/types.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

// Thread safe
class TGroupDowntimes {
private:
    TMutex Mutex;
    THashMap<ui32, TDowntimeHistoryHolder> GroupId2Downtimes;

public:
    auto GetGroupId2Downtimes() const
    {
        TGuard<TMutex> guard(Mutex);

        return GroupId2Downtimes;
    }

    void RegisterDowntime(TInstant now, ui32 groupId);
    void RegisterSuccess(TInstant now, ui32 groupId);
};

}   // namespace NCloud::NBlockStore::NStorage::NPartition
