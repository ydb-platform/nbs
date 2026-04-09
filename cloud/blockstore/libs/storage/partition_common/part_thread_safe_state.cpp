#include "part_thread_safe_state.h"

#include "events_private.h"

#include <cloud/storage/core/libs/actors/helpers.h>
#include <cloud/storage/core/libs/tablet/model/commit.h>

#include <contrib/ydb/library/actors/core/actor.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NPartition;

////////////////////////////////////////////////////////////////////////////////

TPartitionThreadSafeState::TPartitionThreadSafeState(
    ui32 generation,
    ui32 lastCommitId)
{
    Init(generation, lastCommitId);
}

void TPartitionThreadSafeState::Init(ui32 generation, ui32 lastCommitId)
{
    Generation = generation;
    LastCommitId = lastCommitId;
}

ui64 TPartitionThreadSafeState::GenerateCommitId()
{
    TGuard guard(StateLock);
    return GenerateCommitIdImpl();
}

ui64 TPartitionThreadSafeState::GetLastCommitId() const
{
    TGuard guard(StateLock);
    return GetLastCommitIdImpl();
}

ui64 TPartitionThreadSafeState::StartFreshWrite(ui64 blockCount)
{
    TGuard guard(StateLock);

    auto commitId = GenerateCommitIdImpl();

    TrimFreshLogBarriers.AcquireBarrierN(commitId, blockCount);
    return commitId;
}

void TPartitionThreadSafeState::FinishFreshWrite(
    ui64 commitId,
    ui64 blockCount,
    bool isError)
{
    TGuard guard(StateLock);

    if (isError) {
        TrimFreshLogBarriers.ReleaseBarrierN(commitId, blockCount);
    }
}

ui64 TPartitionThreadSafeState::GetTrimFreshLogToCommitId() const
{
    TGuard guard(StateLock);

    return Min(
        GetLastCommitIdImpl(),
        // if there are fresh writes in-flight, trim only up to
        // the smallest in-flight commit id minus one
        TrimFreshLogBarriers.GetMinCommitId() - 1);
}

ui64 TPartitionThreadSafeState::GenerateCommitIdImpl()
{
    if (LastCommitId == Max<ui32>()) {
        return InvalidCommitId;
    }

    ++LastCommitId;
    return MakeCommitId(Generation, LastCommitId);
}

ui64 TPartitionThreadSafeState::GetLastCommitIdImpl() const
{
    return MakeCommitId(Generation, LastCommitId);
}

}   // namespace NCloud::NBlockStore::NStorage
