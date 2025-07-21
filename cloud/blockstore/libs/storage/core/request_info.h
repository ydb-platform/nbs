#pragma once

#include "public.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/kikimr/events.h>
#include <cloud/blockstore/libs/kikimr/helpers.h>
#include <cloud/blockstore/libs/service/context.h>

#include <library/cpp/deprecated/atomic/atomic.h>
#include <library/cpp/lwtrace/shuttle.h>

#include <util/generic/intrlist.h>
#include <util/generic/ptr.h>
#include <util/system/datetime.h>

namespace NCloud::NBlockStore::NStorage {

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

    TCallContextPtr CallContext = MakeIntrusive<TCallContext>();
    TCancelRoutine* CancelRoutine = nullptr;

    const ui64 Started = GetCycleCount();
    ui64 ExecCycles = 0;

    TRequestInfo() = default;

    TRequestInfo(
            const NActors::TActorId& sender,
            ui64 cookie,
            TCallContextPtr callContext)
        : Sender(sender)
        , Cookie(cookie)
        , CallContext(std::move(callContext))
    {
    }

    void CancelRequest(const NActors::TActorContext& ctx)
    {
        if (!CancelRoutine) {
            ReportCancelRoutineIsNotSet(
                TStringBuilder()
                << "CancelRoutine is not set for request. Sender=" << Sender
                << ", Cookie=" << Cookie);
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
        auto exec = GetExecCycles();
        auto total = GetTotalCycles();
        if (total > exec) {
            return total - exec;
        } else {
            return 0;
        }
    }
};

using TRequestInfoPtr = TIntrusivePtr<TRequestInfo>;

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
    return MakeIntrusive<TRequestInfo>(
        sender,
        cookie,
        std::move(callContext));
}

inline TRequestInfoPtr CreateRequestInfo(
    const NActors::TActorId& sender,
    ui64 cookie,
    TCallContextPtr callContext,
    TRequestInfo::TCancelRoutine callback)
{
    auto requestInfo = MakeIntrusive<TRequestInfo>(
        sender,
        cookie,
        std::move(callContext));

    requestInfo->CancelRoutine = callback;

    return requestInfo;
}

template <typename TResponse>
TRequestInfoPtr CreateRequestInfoWithResponse(
    const NActors::TActorId& sender,
    ui64 cookie,
    TCallContextPtr callContext)
{
    auto callback =
        [](const NActors::TActorContext& ctx, TRequestInfo& requestInfo)
    {
        auto response = std::make_unique<TResponse>(
            MakeError(E_REJECTED, "tablet is shutting down"));

        NCloud::Reply(ctx, requestInfo, std::move(response));
    };

    return CreateRequestInfo(
        sender,
        cookie,
        std::move(callContext),
        std::move(callback));
}

template <typename TMethod>
TRequestInfoPtr CreateRequestInfo(
    const NActors::TActorId& sender,
    ui64 cookie,
    TCallContextPtr callContext)
{
    return CreateRequestInfoWithResponse<typename TMethod::TResponse>(
        sender,
        cookie,
        std::move(callContext));
}

}    // namespace NCloud::NBlockStore::NStorage
