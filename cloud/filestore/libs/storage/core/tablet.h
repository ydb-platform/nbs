#pragma once

#include "public.h"

#include <cloud/filestore/libs/service/context.h>
#include <cloud/filestore/libs/storage/core/probes.h>

#include <cloud/storage/core/libs/kikimr/public.h>

#include <ydb/core/base/tablet.h>
#include <ydb/core/tablet_flat/flat_database.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/log.h>

namespace NCloud::NFileStore::NStorage {

LWTRACE_USING(FILESTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

NKikimr::NTabletFlatExecutor::IMiniKQLFactory* NewMiniKQLFactory();

////////////////////////////////////////////////////////////////////////////////

struct ITransactionBase
    : public NKikimr::NTabletFlatExecutor::ITransaction
{
    virtual void Init(const NActors::TActorContext& ctx) = 0;
};

namespace NImpl {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
concept combinedRequest = requires (T v)
{
    {v.Requests};
};

}   // namespace NImpl

#define TX_TRACK_HELPER(probe, info)                                           \
    FILESTORE_TRACK(probe, info->CallContext, TTx::Name);                      \
// TX_TRACK_HELPER

#define TX_TRACK(probe)                                                        \
    if constexpr (NImpl::combinedRequest<typename TTx::TArgs>) {               \
        for (auto& __request: Args.Requests) {                                 \
            TX_TRACK_HELPER(probe, __request.RequestInfo);                     \
        }                                                                      \
    } else if (Args.RequestInfo) {                                             \
        TX_TRACK_HELPER(probe, Args.RequestInfo);                              \
    }                                                                          \
// TX_TRACK

#define TX_FORK_HELPER(request)                                                \
    if (auto& cc = request->CallContext; !cc->LWOrbit.Fork(Orbit)) {           \
        FILESTORE_TRACK(ForkFailed, cc, TTx::Name);                            \
    }                                                                          \
// TX_FORK_HELPER

#define TX_FORK()                                                              \
    if constexpr (NImpl::combinedRequest<typename TTx::TArgs>) {               \
        for (auto& __request: Args.Requests) {                                 \
            TX_FORK_HELPER(__request.RequestInfo);                             \
        }                                                                      \
    } else if (Args.RequestInfo) {                                             \
        TX_FORK_HELPER(Args.RequestInfo);                                      \
    }                                                                          \
// TX_FORK

#define TX_JOIN_HELPER(request)                                                \
        request->CallContext->LWOrbit.Join(Orbit);                             \
// TX_JOIN_HELPER

#define TX_JOIN()                                                              \
    if constexpr (NImpl::combinedRequest<typename TTx::TArgs>) {               \
        for (auto& __request: Args.Requests) {                                 \
            TX_JOIN_HELPER(__request.RequestInfo);                             \
        }                                                                      \
    } else if (Args.RequestInfo) {                                             \
        TX_JOIN_HELPER(Args.RequestInfo);                                      \
    }                                                                          \
// TX_JOIN

////////////////////////////////////////////////////////////////////////////////

template <typename T>
class TTabletBase
    : public NKikimr::NTabletFlatExecutor::TTabletExecutedFlat
{
public:
    TTabletBase(const NActors::TActorId& owner,
                NKikimr::TTabletStorageInfoPtr storage)
        : TTabletExecutedFlat(storage.Get(), owner, NewMiniKQLFactory())
    {}

protected:
    template <typename TTx>
    class TTransaction final
        : public ITransactionBase
    {
    private:
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
            LOG_TRACE(ctx, T::LogComponent,
                "[%lu] PrepareTx %s (gen: %u, step: %u)",
                Self->TabletID(),
                TTx::Name,
                Generation,
                Step);

            if (!TTx::PrepareTx(*Self, ctx, tx, Args)) {
                TX_TRACK(TxPrepareRestarted);

                Args.Clear();
                return false;
            }

            tx.DB.NoMoreReadsForTx();

            TX_TRACK(TxExecute);
            LOG_TRACE(ctx, T::LogComponent,
                "[%lu] ExecuteTx %s (gen: %u, step: %u)",
                Self->TabletID(),
                TTx::Name,
                Generation,
                Step);

            TTx::ExecuteTx(*Self, ctx, tx, Args);
            TX_TRACK(TxExecuteDone);

            return true;
        }

        void Complete(const NActors::TActorContext& ctx) override
        {
            TX_TRACK(TxComplete);
            LOG_TRACE(ctx, T::LogComponent,
                "[%lu] CompleteTx %s (gen: %u, step: %u)",
                Self->TabletID(),
                TTx::Name,
                Generation,
                Step);

            // should join before sending reply in tx complete
            TX_JOIN();

            TTx::CompleteTx(*Self, ctx, Args);
        }
    };

    template <typename TTx, typename ...TArgs>
    std::unique_ptr<TTransaction<TTx>> CreateTx(TArgs&& ...args)
    {
        return std::make_unique<TTransaction<TTx>>(
            static_cast<T*>(this),
            std::forward<TArgs>(args)...);
    }

    template <typename TTx, typename ...TArgs>
    void ExecuteTx(const NActors::TActorContext& ctx, TArgs&& ...args)
    {
        auto tx = CreateTx<TTx>(std::forward<TArgs>(args)...);
        tx->Init(ctx);
        TTabletExecutedFlat::Execute(tx.release(), ctx);
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

#undef TX_FORK
#undef TX_FORK_HELPER

#undef TX_JOIN
#undef TX_JOIN_HELPER

////////////////////////////////////////////////////////////////////////////////

#define FILESTORE_IMPLEMENT_COMMON_TRANSACTION(name, ns)                       \
    void CompleteAndUpdateState(                                               \
        const NActors::TActorContext& ctx,                                     \
        ns::T##name& args)                                                     \
    {                                                                          \
        UpdateInMemoryIndexState(args);                                        \
        CompleteTx_##name(ctx, args);                                          \
    }                                                                          \
// FILESTORE_IMPLEMENT_COMMON_TRANSACTION

#define FILESTORE_IMPLEMENT_RW_TRANSACTION(name, ns)                           \
    struct T##name                                                             \
    {                                                                          \
        using TArgs = ns::T##name;                                             \
                                                                               \
        static constexpr const char* Name = #name;                             \
        static constexpr NKikimr::TTxType TxType = TCounters::TX_##name;       \
        static constexpr bool IsReadOnly = false;                              \
                                                                               \
        template <typename T, typename ...Args>                                \
        static bool PrepareTx(T& target, Args&& ...args)                       \
        {                                                                      \
            return target.PrepareTx_##name(std::forward<Args>(args)...);       \
        }                                                                      \
                                                                               \
        template <typename T, typename ...Args>                                \
        static void ExecuteTx(T& target, Args&& ...args)                       \
        {                                                                      \
            target.ExecuteTx_##name(std::forward<Args>(args)...);              \
        }                                                                      \
                                                                               \
        template <typename T, typename ...Args>                                \
        static void CompleteTx(T& target, Args&& ...args)                      \
        {                                                                      \
            target.CompleteAndUpdateState(std::forward<Args>(args)...);        \
        }                                                                      \
    };                                                                         \
                                                                               \
    bool PrepareTx_##name(                                                     \
        const NActors::TActorContext& ctx,                                     \
        NKikimr::NTabletFlatExecutor::TTransactionContext& tx,                 \
        ns::T##name& args);                                                    \
                                                                               \
    void ExecuteTx_##name(                                                     \
        const NActors::TActorContext& ctx,                                     \
        NKikimr::NTabletFlatExecutor::TTransactionContext& tx,                 \
        ns::T##name& args);                                                    \
                                                                               \
    void CompleteTx_##name(                                                    \
        const NActors::TActorContext& ctx,                                     \
        ns::T##name& args);                                                    \
                                                                               \
    FILESTORE_IMPLEMENT_COMMON_TRANSACTION(name, ns)                           \
// FILESTORE_IMPLEMENT_RW_TRANSACTION

// For RO transactions we allow to alternatively declare ValidateTx_, PrepareTx_
// and CompleteTx_, where Validate and PrepareTx are two stages of Prepare (one
// not using, and one using db). CompleteTx is the same for all other
// transactions. ValidateTx is expected to return false if Execute is to be
// executed, and true if it is not.
//
// Unlike FILESTORE_IMPLEMENT_RW_TRANSACTION, this macro allows to define
// operations that can both run atop of the LocalDB and other implementations
// of the database. Thus, signature of ExecuteTx_ is a bit more lax.
//
// This macro also provides TryExecuteTx function that will run the whole
// transaction and call CompleteTx_ if it was successful.
#define FILESTORE_IMPLEMENT_RO_TRANSACTION(name, ns, dbType, dbIfaceType)      \
    struct T##name                                                             \
    {                                                                          \
        using TArgs = ns::T##name;                                             \
                                                                               \
        static constexpr const char* Name = #name;                             \
        static constexpr NKikimr::TTxType TxType = TCounters::TX_##name;       \
        static constexpr bool IsReadOnly = true;                               \
                                                                               \
        template <typename T>                                                  \
        static bool PrepareTx(                                                 \
            T& target,                                                         \
            const NActors::TActorContext& ctx,                                 \
            NKikimr::NTabletFlatExecutor::TTransactionContext& tx,             \
            ns::T##name& args)                                                 \
        {                                                                      \
            if (target.ValidateTx_##name(ctx, args)) {                         \
                dbType db(tx.DB, args.NodeUpdates);                            \
                return target.PrepareTx_##name(ctx, db, args);                 \
            }                                                                  \
            return true;                                                       \
        }                                                                      \
                                                                               \
        template <typename T>                                                  \
        static void ExecuteTx(                                                 \
            T& target,                                                         \
            const NActors::TActorContext& ctx,                                 \
            NKikimr::NTabletFlatExecutor::TTransactionContext& tx,             \
            ns::T##name& args)                                                 \
        {                                                                      \
            Y_UNUSED(target, ctx, tx, args);                                   \
        }                                                                      \
                                                                               \
        template <typename T>                                                  \
        static void CompleteTx(                                                \
            T& target,                                                         \
            const NActors::TActorContext& ctx,                                 \
            ns::T##name& args)                                                 \
        {                                                                      \
            target.CompleteAndUpdateState(ctx, args);                          \
        }                                                                      \
    };                                                                         \
                                                                               \
    bool ValidateTx_##name(                                                    \
        const NActors::TActorContext& ctx,                                     \
        ns::T##name& args);                                                    \
                                                                               \
    bool PrepareTx_##name(                                                     \
        const NActors::TActorContext& ctx,                                     \
        dbIfaceType& db,                                                       \
        ns::T##name& args);                                                    \
                                                                               \
    void CompleteTx_##name(                                                    \
        const NActors::TActorContext& ctx,                                     \
        ns::T##name& args);                                                    \
                                                                               \
    bool TryExecuteTx(                                                         \
        const NActors::TActorContext& ctx,                                     \
        dbIfaceType& db,                                                       \
        ns::T##name& args)                                                     \
    {                                                                          \
        if (!ValidateTx_##name(ctx, args) || PrepareTx_##name(ctx, db, args)) {\
            CompleteTx_##name(ctx, args);                                      \
            return true;                                                       \
        }                                                                      \
        args.Clear();                                                          \
        return false;                                                          \
    }                                                                          \
                                                                               \
    FILESTORE_IMPLEMENT_COMMON_TRANSACTION(name, ns)                           \
// FILESTORE_IMPLEMENT_RO_TRANSACTION

}   // namespace NCloud::NFileStore::NStorage
