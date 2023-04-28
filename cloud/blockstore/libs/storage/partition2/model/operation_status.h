#pragma once

namespace NCloud::NBlockStore::NStorage::NPartition2 {

////////////////////////////////////////////////////////////////////////////////

enum class EOperationStatus
{
    Idle,
    Enqueued,
    Delayed,
    Started,
};

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
