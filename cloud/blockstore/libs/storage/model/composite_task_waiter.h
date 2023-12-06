#pragma once

#include <cloud/storage/core/libs/actors/public.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/event.h>

namespace NCloud::NBlockStore::NStorage {

///////////////////////////////////////////////////////////////////////////////

using TPrincipalTaskId = ui64;
using TDependentTaskId = ui64;

constexpr TPrincipalTaskId INVALID_TASK_ID = 0;

///////////////////////////////////////////////////////////////////////////////

class TWaitDependentAndReply
{
    NActors::TActorId Recipient;
    NActors::IEventBasePtr Response;
    ui32 Flags = 0;
    ui64 Cookie = 0;

    ui32 WaitCount = 0;
    TPrincipalTaskId PrincipalTaskId = INVALID_TASK_ID;

public:
    TWaitDependentAndReply(TPrincipalTaskId principalTaskId)
        : PrincipalTaskId(principalTaskId)
    {}
    TWaitDependentAndReply(const TWaitDependentAndReply&) = delete;
    TWaitDependentAndReply(TWaitDependentAndReply&&) = default;
    ~TWaitDependentAndReply() = default;

    TWaitDependentAndReply& operator=(const TWaitDependentAndReply&) = delete;
    TWaitDependentAndReply& operator=(TWaitDependentAndReply&&) = default;

    template <typename T>
    void ArmReply(T& request, NActors::IEventBasePtr response)
    {
        Response = std::move(response);
        Recipient = request.Sender;
        Cookie = request.Cookie;
    }

    TPrincipalTaskId GetPrincipalTaskId() const
    {
        return PrincipalTaskId;
    }

    void IncCounter();
    void DecCounter(const NActors::TActorContext& ctx);
    bool IsDone() const;

private:
    void FinishIfReady(const NActors::TActorContext& ctx);
};

///////////////////////////////////////////////////////////////////////////////

class TCompositeTaskList
{
    TPrincipalTaskId PrincipalIdGenerator = 1000;
    TDependentTaskId DependentIdGenerator = 2000;
    TMap<TPrincipalTaskId, TWaitDependentAndReply> PrincipalTasks;
    TMap<TDependentTaskId, TPrincipalTaskId> DependentTasks;

public:
    TWaitDependentAndReply* StartPrincipalTask();

    TDependentTaskId StartDependentTaskAwait(TPrincipalTaskId principalTaskId);
    void FinishDependentTaskAwait(
        TDependentTaskId dependentTaskId,
        const NActors::TActorContext& ctx);
};

///////////////////////////////////////////////////////////////////////////////

} // namespace NCloud::NBlockStore::NStorage
