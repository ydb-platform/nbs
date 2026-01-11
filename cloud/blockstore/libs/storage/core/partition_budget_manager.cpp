#include "partition_budget_manager.h"

#include "config.h"

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

TPartitionBudgetManager::TPartitionBudgetManager(TStorageConfigPtr config)
    : Config(std::move(config))
{}

TPartitionBudgetManager::~TPartitionBudgetManager() = default;

bool TPartitionBudgetManager::HasEnoughDirectWriteBudget(
    TInstant ts,
    ui32 byteCount)
{
    if (!Config->GetMultiAgentWriteEnabled() ||
        byteCount < Config->GetMultiAgentWriteRequestSizeThreshold())
    {
        return true;
    }

    const ui64 quota = Config->GetDirectWriteBandwidthQuota();
    if (quota == 0) {
        return false;
    }

    auto guard = Guard(Lock);
    return DirectWriteBandwidthQuota.Register(
               ts,
               static_cast<double>(byteCount) / quota) == 0;
}

}   // namespace NCloud::NBlockStore::NStorage
