#include "test_tablet.h"

#include <cloud/blockstore/libs/kikimr/helpers.h>
#include <cloud/blockstore/libs/storage/api/partition.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/volume.h>

#include <ydb/core/blockstore/core/blockstore.h>
#include <ydb/core/base/tablet.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

using namespace NCloud::NBlockStore::NStorage::NPartition;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TTabletActor final
    : public TActor<TTabletActor>
    , public NTabletFlatExecutor::TTabletExecutedFlat
{
public:
    TTabletActor(const TActorId& owner, TTabletStorageInfoPtr storage)
        : TActor(&TThis::StateBoot)
        , TTabletExecutedFlat(storage.Get(), owner, nullptr)
    {}

private:
    template <typename T>
    void BecomeAux(const TActorContext& ctx, const char* name, T state)
    {
        Become(state);
        OnStateChanged(ctx, name);
    }

    void OnStateChanged(const TActorContext& ctx, const char* name);

    void DefaultSignalTabletActive(const TActorContext& ctx) override;
    void OnActivateExecutor(const TActorContext& ctx) override;

    void OnDetach(const TActorContext& ctx) override;

    void OnTabletDead(
        TEvTablet::TEvTabletDead::TPtr& ev,
        const TActorContext& ctx) override;

private:
    STFUNC(StateBoot);
    STFUNC(StateWork);
    STFUNC(StateZombie);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void HandleUpdateVolumeConfig(
        TEvBlockStore::TEvUpdateVolumeConfig::TPtr& ev,
        const TActorContext& ctx);

    template <typename TRequest, typename TResponse>
    void ForwardRequest(
        const char* name,
        const typename TRequest::TPtr& ev,
        const TActorContext& ctx);

    bool HandleRequests(STFUNC_SIG);

    BLOCKSTORE_SERVICE_REQUESTS(BLOCKSTORE_IMPLEMENT_REQUEST, TEvService)
    BLOCKSTORE_VOLUME_REQUESTS(BLOCKSTORE_IMPLEMENT_REQUEST, TEvVolume)
    BLOCKSTORE_PARTITION_REQUESTS(BLOCKSTORE_IMPLEMENT_REQUEST, TEvPartition)
};

////////////////////////////////////////////////////////////////////////////////

void TTabletActor::OnStateChanged(const TActorContext& ctx, const char* name)
{
    LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
        "[%lu] Switched to state %s (system: %s, user: %s, executor: %s)",
        TabletID(),
        name,
        ToString(Tablet()).data(),
        ToString(SelfId()).data(),
        ToString(ExecutorID()).data());
}

void TTabletActor::DefaultSignalTabletActive(const TActorContext&)
{
    // must be empty
}

void TTabletActor::OnActivateExecutor(const TActorContext& ctx)
{
    SignalTabletActive(ctx);
    BecomeAux(ctx, "Work", &TThis::StateWork);
}

void TTabletActor::OnDetach(const TActorContext& ctx)
{
    Die(ctx);
}

void TTabletActor::OnTabletDead(
    TEvTablet::TEvTabletDead::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    Die(ctx);
}

void TTabletActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    NCloud::Send<TEvents::TEvPoisonPill>(ctx, Tablet());
    BecomeAux(ctx, "Zombie", &TThis::StateZombie);
}

void TTabletActor::HandleUpdateVolumeConfig(
    TEvBlockStore::TEvUpdateVolumeConfig::TPtr& ev,
    const TActorContext& ctx)
{
    const auto& request = ev->Get()->Record;

    auto response = std::make_unique<TEvBlockStore::TEvUpdateVolumeConfigResponse>();
    response->Record.SetTxId(request.GetTxId());
    response->Record.SetOrigin(TabletID());
    response->Record.SetStatus(NKikimrBlockStore::OK);

    NCloud::Reply(ctx, *ev, std::move(response));
}

template <typename TRequest, typename TResponse>
void TTabletActor::ForwardRequest(
    const char* name,
    const typename TRequest::TPtr& ev,
    const TActorContext& ctx)
{
    LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
        "[%lu] Receieved request: %s",
        TabletID(),
        name);

    NCloud::Reply(ctx, *ev, std::make_unique<TResponse>());
}

#define BLOCKSTORE_FORWARD_REQUEST(name, ns)                                   \
    void TTabletActor::Handle##name(                                           \
        const ns::TEv##name##Request::TPtr& ev,                                \
        const TActorContext& ctx)                                              \
    {                                                                          \
        ForwardRequest<                                                        \
            ns::TEv##name##Request,                                            \
            ns::TEv##name##Response>(#name, ev, ctx);                          \
    }                                                                          \
// BLOCKSTORE_FORWARD_REQUEST

BLOCKSTORE_SERVICE_REQUESTS(BLOCKSTORE_FORWARD_REQUEST, TEvService)
BLOCKSTORE_VOLUME_REQUESTS(BLOCKSTORE_FORWARD_REQUEST, TEvVolume)
BLOCKSTORE_PARTITION_REQUESTS(BLOCKSTORE_FORWARD_REQUEST, TEvPartition)

#undef BLOCKSTORE_FORWARD_REQUEST

bool TTabletActor::HandleRequests(STFUNC_SIG)
{
    switch (ev->GetTypeRewrite()) {
        BLOCKSTORE_SERVICE_REQUESTS(BLOCKSTORE_HANDLE_REQUEST, TEvService)
        BLOCKSTORE_VOLUME_REQUESTS(BLOCKSTORE_HANDLE_REQUEST, TEvVolume)
        BLOCKSTORE_PARTITION_REQUESTS(BLOCKSTORE_HANDLE_REQUEST, TEvPartition)

        default:
            return false;
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TTabletActor::StateBoot)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        IgnoreFunc(TEvTabletPipe::TEvServerConnected);
        IgnoreFunc(TEvTabletPipe::TEvServerDisconnected);

        HFunc(TEvBlockStore::TEvUpdateVolumeConfig, HandleUpdateVolumeConfig);

        default:
            StateInitImpl(ev, SelfId());
            break;
    }
}

STFUNC(TTabletActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        IgnoreFunc(TEvTabletPipe::TEvServerConnected);
        IgnoreFunc(TEvTabletPipe::TEvServerDisconnected);

        HFunc(TEvBlockStore::TEvUpdateVolumeConfig, HandleUpdateVolumeConfig);

        default:
            if (!HandleRequests(ev) &&
                !HandleDefaultEvents(ev, SelfId()))
            {
                HandleUnexpectedEvent(
                    ev,
                    TBlockStoreComponents::PARTITION,
                    __PRETTY_FUNCTION__);
            }
            break;
    }
}

STFUNC(TTabletActor::StateZombie)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvTablet::TEvTabletDead, HandleTabletDead);

        IgnoreFunc(TEvTabletPipe::TEvServerConnected);
        IgnoreFunc(TEvTabletPipe::TEvServerDisconnected);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::PARTITION,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IActorPtr CreateTestTablet(const TActorId& owner, TTabletStorageInfoPtr storage)
{
    return std::make_unique<TTabletActor>(owner, std::move(storage));
}

}   // namespace NCloud::NBlockStore::NStorage
