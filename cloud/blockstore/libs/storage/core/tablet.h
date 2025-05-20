#pragma once

#include "public.h"

#include "probes.h"

#include <cloud/blockstore/libs/kikimr/events.h>

#include <contrib/ydb/core/base/tablet.h>
#include <contrib/ydb/core/tablet_flat/flat_database.h>
#include <contrib/ydb/core/tablet_flat/tablet_flat_executed.h>

#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/actors/core/log.h>

#include <type_traits>

namespace NCloud::NBlockStore::NStorage {

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

NKikimr::NTabletFlatExecutor::IMiniKQLFactory* NewMiniKQLFactory();

ui64 CreateTransactionId();

////////////////////////////////////////////////////////////////////////////////

class ITransactionTracker
{
public:
    virtual ~ITransactionTracker() = default;

    virtual void OnStarted(
        ui64 transactionId,
        TString transactionName,
        ui64 startTime) = 0;
    virtual void OnFinished(ui64 transactionId, ui64 finishTime) = 0;
};

struct ITransactionBase: public NKikimr::NTabletFlatExecutor::ITransaction
{
    virtual void Init(const NActors::TActorContext& ctx) = 0;
};

////////////////////////////////////////////////////////////////////////////////

template <typename T, typename = void>
constexpr bool combinedRequest = false;

template <typename T>
constexpr bool combinedRequest<T, std::void_t<
    decltype(std::declval<T>().Requests)>> = true;

template <typename T>
constexpr bool HasRequestInfoField() {
    return requires(const T& t) {
        t.RequestInfo;
    };
}

#define TX_TRACK_HELPER(probe, info)                                           \
    LWTRACK(                                                                   \
        probe,                                                                 \
        info->CallContext->LWOrbit,                                            \
        TTx::Name,                                                             \
        info->CallContext->RequestId);                                         \
// TX_TRACK_HELPER

#define TX_TRACK(probe)                                                        \
    if constexpr (combinedRequest<typename TTx::TArgs>) {                      \
        for (auto& __request: Args.Requests) {                                 \
            TX_TRACK_HELPER(probe, __request.RequestInfo);                     \
        }                                                                      \
    } else if constexpr (HasRequestInfoField<decltype(Args)>()) {              \
        if (Args.RequestInfo) {                                                \
            TX_TRACK_HELPER(probe, Args.RequestInfo);                          \
        }                                                                      \
    }                                                                          \
// TX_TRACK

#define TX_FORK_HELPER(request)                                                \
    if (auto& cc = request->CallContext; !cc->LWOrbit.Fork(Orbit)) {           \
        LWTRACK(ForkFailed, cc->LWOrbit, TTx::Name, cc->RequestId);            \
    }                                                                          \
// TX_FORK_HELPER

#define TX_FORK()                                                              \
    if constexpr (combinedRequest<typename TTx::TArgs>) {                      \
        for (auto& __request: Args.Requests) {                                 \
            TX_FORK_HELPER(__request.RequestInfo);                             \
        }                                                                      \
    } else if constexpr (HasRequestInfoField<decltype(Args)>()) {              \
        if (Args.RequestInfo) {                                                \
            TX_FORK_HELPER(Args.RequestInfo);                                  \
        }                                                                      \
    }                                                                          \
// TX_FORK

#define TX_JOIN_HELPER(request)                                                \
        request->CallContext->LWOrbit.Join(Orbit);                             \
// TX_JOIN_HELPER

#define TX_JOIN()                                                              \
    if constexpr (combinedRequest<typename TTx::TArgs>) {                      \
        for (auto& __request: Args.Requests) {                                 \
            TX_JOIN_HELPER(__request.RequestInfo);                             \
        }                                                                      \
    } else if constexpr (HasRequestInfoField<decltype(Args)>()) {              \
        if (Args.RequestInfo) {                                                \
            TX_JOIN_HELPER(Args.RequestInfo);                                  \
        }                                                                      \
    }                                                                          \
// TX_JOIN

template <typename T>
class TTabletBase
    : public NKikimr::NTabletFlatExecutor::TTabletExecutedFlat
{
    ITransactionTracker* TransactionTracker = nullptr;

public:
    TTabletBase(const NActors::TActorId& owner,
                NKikimr::TTabletStorageInfoPtr storage,
                ITransactionTracker* transactionTracker)
        : TTabletExecutedFlat(storage.Get(), owner, NewMiniKQLFactory())
        , TransactionTracker(transactionTracker)
    {}

protected:
    template <typename TTx>
    class TTransaction final
        : public ITransactionBase
    {
    private:
        const ui64 TransactionId = CreateTransactionId();

        T* Self;
        typename TTx::TArgs Args;

        ui32 Generation = 0;
        ui32 Step = 0;

    public:
        template <typename ...TArgs>
        TTransaction(T* self, TArgs&& ...args)
            : Self(self)
            , Args(std::forward<TArgs>(args)...)
        {}

        NKikimr::TTxType GetTxType() const override
        {
            return TTx::TxType;
        }

        void Init(const NActors::TActorContext&) override
        {
            if (Self->TransactionTracker) {
                Self->TransactionTracker->OnStarted(
                    TransactionId,
                    TTx::Name,
                    GetCycleCount());
            }

            TX_TRACK(TxInit);
            TX_FORK();
        }

        bool Execute(
            NKikimr::NTabletFlatExecutor::TTransactionContext& tx,
            const NActors::TActorContext& ctx) override
        {
            Generation = tx.Generation;
            Step = tx.Step;

            TX_TRACK(TxPrepare);

            LOG_DEBUG(ctx, T::LogComponent,
                "[%lu] Prepare %s (gen: %u, step: %u)",
                Self->TabletID(),
                TTx::Name,
                Generation,
                Step);

            if (!TTx::Prepare(*Self, ctx, tx, Args)) {
                Args.Clear();
                TX_TRACK(TxPrepareRestarted);
                return false;
            }

            tx.DB.NoMoreReadsForTx();

            TX_TRACK(TxExecute);

            LOG_DEBUG(ctx, T::LogComponent,
                "[%lu] Execute %s (gen: %u, step: %u)",
                Self->TabletID(),
                TTx::Name,
                Generation,
                Step);

            TTx::Execute(*Self, ctx, tx, Args);

            TX_TRACK(TxExecuteDone);

            return true;
        }

        void Complete(const NActors::TActorContext& ctx) override
        {
            if (Self->TransactionTracker) {
                Self->TransactionTracker->OnFinished(
                    TransactionId,
                    GetCycleCount());
            }

            TX_TRACK(TxComplete);

            LOG_DEBUG(ctx, T::LogComponent,
                "[%lu] Complete %s (gen: %u, step: %u)",
                Self->TabletID(),
                TTx::Name,
                Generation,
                Step);

            TX_JOIN();

            TTx::Complete(*Self, ctx, Args);
        }
    };

    template <typename TTx, typename ...TArgs>
    std::unique_ptr<TTransaction<TTx>> CreateTx(TArgs&& ...args)
    {
        return std::make_unique<TTransaction<TTx>>(
            static_cast<T*>(this),
            std::forward<TArgs>(args)...);
    }

    template <typename TTx, typename... TArgs>
    void ExecuteTx(const NActors::TActorContext& ctx, TArgs&&... args)
    {
        ExecuteTx(ctx, CreateTx<TTx>(std::forward<TArgs>(args)...));
    }

    void ExecuteTx(
        const NActors::TActorContext& ctx,
        std::unique_ptr<ITransactionBase> tx)
    {
        tx->Init(ctx);
        TTabletExecutedFlat::Execute(tx.release(), ctx);
    }
};

#undef TX_TRACK
#undef TX_TRACK_HELPER

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_IMPLEMENT_TRANSACTION(name, ns)                             \
    struct T##name                                                             \
    {                                                                          \
        using TArgs = ns::T##name;                                             \
                                                                               \
        static constexpr const char* Name = #name;                             \
        static constexpr NKikimr::TTxType TxType = TCounters::TX_##name;       \
                                                                               \
        template <typename T, typename ...Args>                                \
        static bool Prepare(T& target, Args&& ...args)                         \
        {                                                                      \
            return target.Prepare##name(std::forward<Args>(args)...);          \
        }                                                                      \
                                                                               \
        template <typename T, typename ...Args>                                \
        static void Execute(T& target, Args&& ...args)                         \
        {                                                                      \
            target.Execute##name(std::forward<Args>(args)...);                 \
        }                                                                      \
                                                                               \
        template <typename T, typename ...Args>                                \
        static void Complete(T& target, Args&& ...args)                        \
        {                                                                      \
            target.Complete##name(std::forward<Args>(args)...);                \
        }                                                                      \
    };                                                                         \
                                                                               \
    bool Prepare##name(                                                        \
        const NActors::TActorContext& ctx,                                     \
        NKikimr::NTabletFlatExecutor::TTransactionContext& tx,                 \
        ns::T##name& args);                                                    \
                                                                               \
    void Execute##name(                                                        \
        const NActors::TActorContext& ctx,                                     \
        NKikimr::NTabletFlatExecutor::TTransactionContext& tx,                 \
        ns::T##name& args);                                                    \
                                                                               \
    void Complete##name(                                                       \
        const NActors::TActorContext& ctx,                                     \
        ns::T##name& args);                                                    \
// BLOCKSTORE_IMPLEMENT_TRANSACTION

}   // namespace NCloud::NBlockStore::NStorage
