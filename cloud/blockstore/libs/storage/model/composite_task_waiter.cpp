#include "composite_task_waiter.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>

namespace NCloud::NBlockStore::NStorage {

void TWaitDependentAndReply::IncCounter()
{
    ++WaitCount;
}

void TWaitDependentAndReply::DecCounter(const NActors::TActorContext& ctx)
{
    Y_VERIFY_DEBUG(WaitCount != 0);
    --WaitCount;
    FinishIfReady(ctx);
}

bool TWaitDependentAndReply::IsDone() const
{
    return WaitCount == 0;
}

void TWaitDependentAndReply::FinishIfReady(const NActors::TActorContext& ctx)
{
    if (WaitCount != 0) {
        return;
    }
    Y_VERIFY_DEBUG(Response);
    ctx.Send(Recipient, Response.release(), Flags, Cookie);
}

///////////////////////////////////////////////////////////////////////////////

TWaitDependentAndReply* TCompositeTaskList::StartPrincipalTask()
{
    const TPrincipalTaskId newCompositeTaskId = PrincipalIdGenerator++;
    auto [it, inserted] = PrincipalTasks.emplace(
        newCompositeTaskId,
        TWaitDependentAndReply(newCompositeTaskId));
    Y_VERIFY(inserted);
    return &it->second;
}

TDependentTaskId TCompositeTaskList::StartDependentTaskAwait(
    TPrincipalTaskId principalTaskId)
{
    if (principalTaskId == INVALID_TASK_ID) {
        return INVALID_TASK_ID;
    }

    auto* compositeTask = PrincipalTasks.FindPtr(principalTaskId);
    Y_VERIFY_DEBUG(compositeTask);
    if (!compositeTask) {
        ReportReceivedUnknownTaskId();
        return INVALID_TASK_ID;
    }
    compositeTask->IncCounter();

    const TDependentTaskId newDependentTaskId = DependentIdGenerator++;
    auto [it, inserted] =
        DependentTasks.emplace(newDependentTaskId, principalTaskId);
    Y_VERIFY(inserted);
    return newDependentTaskId;
}

void TCompositeTaskList::FinishDependentTaskAwait(
    TDependentTaskId dependentTaskId,
    const NActors::TActorContext& ctx)
{
    if (dependentTaskId == INVALID_TASK_ID) {
        return;
    }

    auto nestedTaskIt = DependentTasks.find(dependentTaskId);
    Y_VERIFY_DEBUG(nestedTaskIt != DependentTasks.end());
    if (nestedTaskIt == DependentTasks.end()) {
        ReportReceivedUnknownTaskId();
        return;
    }
    auto compositeTaskId = nestedTaskIt->second;
    DependentTasks.erase(nestedTaskIt);

    auto compositeTaskIt = PrincipalTasks.find(compositeTaskId);
    Y_VERIFY(compositeTaskIt != PrincipalTasks.end());
    auto& compositeTask = compositeTaskIt->second;
    compositeTask.DecCounter(ctx);
    if (compositeTask.IsDone()) {
        PrincipalTasks.erase(compositeTaskIt);
    }
}

} // namespace NCloud::NBlockStore::NStorage
