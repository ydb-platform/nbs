#pragma once

#include "public.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>
#include <cloud/filestore/libs/service/context.h>
#include <cloud/filestore/libs/storage/api/events.h>

#include <cloud/storage/core/libs/kikimr/helpers.h>

#include <library/cpp/deprecated/atomic/atomic.h>
#include <library/cpp/lwtrace/shuttle.h>

#include <util/generic/intrlist.h>
#include <util/system/datetime.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TRequestInfo
    : public TAtomicRefCount<TRequestInfo>
    , public TIntrusiveListItem<TRequestInfo>
{
    using TCancelRoutine = void(
        const NActors::TActorContext& ctx,
        TRequestInfo& requestInfo);

    const NActors::TActorId Sender;
    const ui64 Cookie = 0;

    TCallContextPtr CallContext;
    TCancelRoutine* CancelRoutine = nullptr;

    const ui64 Started = GetCycleCount();
    ui64 ExecCycles = 0;

    // ActorSystem start ts. Might be empty.
    TInstant StartedTs;

    TRequestInfo() = default;

    TRequestInfo(
        const NActors::TActorId& sender,
        ui64 cookie,
        TCallContextPtr callContext)
        : Sender(sender)
        , Cookie(cookie)
        , CallContext(std::move(callContext))
    {}

    void CancelRequest(const NActors::TActorContext& ctx)
    {
        if (!CancelRoutine) {
            ReportCancelRoutineIsNotSet();
            return;
        };
        CancelRoutine(ctx, *this);
    }

    void AddExecCycles(ui64 cycles)
    {
        AtomicAdd(ExecCycles, cycles);
    }

    ui64 GetTotalCycles() const
    {
        return GetCycleCount() - Started;
    }

    ui64 GetExecCycles() const
    {
        return AtomicGet(ExecCycles);
    }

    ui64 GetWaitCycles() const
    {
        return GetTotalCycles() - GetExecCycles();
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TRequestScope
{
    TRequestInfo& RequestInfo;

    const ui64 Started = GetCycleCount();

    TRequestScope(TRequestInfo& requestInfo)
        : RequestInfo(requestInfo)
    {}

    ~TRequestScope()
    {
        Finish();
    }

    ui64 Finish()
    {
        return AtomicAdd(RequestInfo.ExecCycles, GetCycleCount() - Started);
    }
};

////////////////////////////////////////////////////////////////////////////////

inline TRequestInfoPtr CreateRequestInfo(
    const NActors::TActorId& sender,
    ui64 cookie,
    TCallContextPtr callContext)
{
    return MakeIntrusive<TRequestInfo>(sender, cookie, std::move(callContext));
}

template <typename TMethod>
TRequestInfoPtr CreateRequestInfo(
    const NActors::TActorId& sender,
    ui64 cookie,
    TCallContextPtr callContext)
{
    auto requestInfo =
        MakeIntrusive<TRequestInfo>(sender, cookie, std::move(callContext));

    requestInfo->CancelRoutine =
        [](const NActors::TActorContext& ctx, TRequestInfo& requestInfo)
    {
        auto response = std::make_unique<typename TMethod::TResponse>(
            MakeError(E_REJECTED, "tablet is shutting down"));

        NCloud::Reply(ctx, requestInfo, std::move(response));
    };

    return requestInfo;
}

}   // namespace NCloud::NFileStore::NStorage
