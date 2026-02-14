#include "group_downtimes.h"

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

void TGroupDowntimes::RegisterDowntime(TInstant now, ui32 groupId)
{
    TGuard guard(Lock);
    GroupId2Downtimes[groupId].PushBack(now, EDowntimeStateChange::DOWN);
}

void TGroupDowntimes::RegisterSuccess(TInstant now, ui32 groupId)
{
    TGuard guard(Lock);
    auto it = GroupId2Downtimes.find(groupId);
    if (it != GroupId2Downtimes.end()) {
        it->second.PushBack(now, EDowntimeStateChange::UP);
        if (!it->second.HasRecentState(now, EDowntimeStateChange::DOWN)) {
            GroupId2Downtimes.erase(it);
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
