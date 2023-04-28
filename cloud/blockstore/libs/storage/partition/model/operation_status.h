#pragma once

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

enum class EOperationStatus
{
    Idle,
    Enqueued,
    Started,
};

}   // namespace NCloud::NBlockStore::NStorage::NPartition
