#pragma once

#include <cloud/blockstore/libs/diagnostics/downtime_history.h>
#include <cloud/blockstore/libs/storage/partition_common/model/public.h>

#include <util/generic/hash.h>
#include <util/system/mutex.h>
#include <util/system/spinlock.h>
#include <util/system/types.h>

namespace NCloud::NBlockStore::NStorage {

// Thread safe
class TGroupDowntimes {
private:
    TAdaptiveLock Lock;
    THashMap<ui32, TDowntimeHistoryHolder> GroupId2Downtimes;

public:
    auto GetGroupId2Downtimes() const
    {
        TGuard guard(Lock);

        return GroupId2Downtimes;
    }

    void RegisterDowntime(TInstant now, ui32 groupId);
    void RegisterSuccess(TInstant now, ui32 groupId);
};

using TGroupDowntimesPtr = std::shared_ptr<TGroupDowntimes>;

}   // namespace NCloud::NBlockStore::NStorage
