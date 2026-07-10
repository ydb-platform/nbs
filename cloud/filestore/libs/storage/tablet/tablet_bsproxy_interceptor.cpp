#include "tablet_bsproxy_interceptor.h"

#include <cloud/filestore/libs/storage/api/components.h>

#include <contrib/ydb/core/base/blobstorage.h>

#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/actors/core/log.h>

#include <util/generic/hash_set.h>
#include <util/generic/singleton.h>
#include <util/system/guard.h>
#include <util/system/mutex.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;
using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TBSProxyInterceptorActor final
    : public TActor<TBSProxyInterceptorActor>
{
private:
    const TActorId RealProxy;
    const ui32 GroupId;

public:
    TBSProxyInterceptorActor(TActorId realProxy, ui32 groupId)
        : TActor(&TThis::StateWork)
        , RealProxy(realProxy)
        , GroupId(groupId)
    {}

private:
    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            case TEvents::TEvPoisonPill::EventType: {
                Die(TActivationContext::AsActorContext());
                return;
            }

            case TEvBlobStorage::EvPut: {
                const auto* msg = ev->Get<TEvBlobStorage::TEvPut>();
                LOG_INFO_S(*TlsActivationContext, TFileStoreComponents::TABLET,
                    "[BSProxyInterceptor] group " << GroupId
                    << " TEvPut blob " << msg->Id.ToString()
                    << " channel " << msg->Id.Channel()
                    << " size " << msg->Id.BlobSize()
                    << " sender " << ev->Sender.ToString());
                break;
            }

            case TEvBlobStorage::EvGet: {
                const auto* msg = ev->Get<TEvBlobStorage::TEvGet>();
                LOG_INFO_S(*TlsActivationContext, TFileStoreComponents::TABLET,
                    "[BSProxyInterceptor] group " << GroupId
                    << " TEvGet queries " << msg->QuerySize
                    << " first " << (msg->QuerySize
                        ? msg->Queries[0].Id.ToString()
                        : TString("none"))
                    << " indexOnly " << msg->IsIndexOnly
                    << " sender " << ev->Sender.ToString());
                break;
            }

            case TEvBlobStorage::EvRange: {
                const auto* msg = ev->Get<TEvBlobStorage::TEvRange>();
                LOG_INFO_S(*TlsActivationContext, TFileStoreComponents::TABLET,
                    "[BSProxyInterceptor] group " << GroupId
                    << " TEvRange tablet " << msg->TabletId
                    << " from " << msg->From.ToString()
                    << " to " << msg->To.ToString()
                    << " sender " << ev->Sender.ToString());
                break;
            }

            case TEvBlobStorage::EvCollectGarbage: {
                const auto* msg = ev->Get<TEvBlobStorage::TEvCollectGarbage>();
                LOG_INFO_S(*TlsActivationContext, TFileStoreComponents::TABLET,
                    "[BSProxyInterceptor] group " << GroupId
                    << " TEvCollectGarbage tablet " << msg->TabletId
                    << " channel " << msg->Channel
                    << " gen " << msg->RecordGeneration
                    << " sender " << ev->Sender.ToString());
                break;
            }

            case TEvBlobStorage::EvBlock: {
                const auto* msg = ev->Get<TEvBlobStorage::TEvBlock>();
                LOG_INFO_S(*TlsActivationContext, TFileStoreComponents::TABLET,
                    "[BSProxyInterceptor] group " << GroupId
                    << " TEvBlock tablet " << msg->TabletId
                    << " gen " << msg->Generation
                    << " sender " << ev->Sender.ToString());
                break;
            }

            case TEvBlobStorage::EvDiscover: {
                const auto* msg = ev->Get<TEvBlobStorage::TEvDiscover>();
                LOG_INFO_S(*TlsActivationContext, TFileStoreComponents::TABLET,
                    "[BSProxyInterceptor] group " << GroupId
                    << " TEvDiscover tablet " << msg->TabletId
                    << " sender " << ev->Sender.ToString());
                break;
            }

            default: {
                LOG_INFO_S(*TlsActivationContext, TFileStoreComponents::TABLET,
                    "[BSProxyInterceptor] group " << GroupId
                    << " event type " << ev->GetTypeRewrite()
                    << " sender " << ev->Sender.ToString());
                break;
            }
        }

        TActivationContext::Forward(ev, RealProxy);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TInterceptedGroups
{
    TMutex Lock;
    THashSet<ui64> Keys;   // nodeId << 32 | groupId
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void EnsureBSProxyInterceptors(
    const TActorContext& ctx,
    const TTabletStorageInfo& info)
{
    auto* registry = Singleton<TInterceptedGroups>();
    auto* actorSystem = TActivationContext::ActorSystem();
    const ui32 nodeId = ctx.SelfID.NodeId();

    THashSet<ui32> groups;
    for (const auto& channel: info.Channels) {
        for (const auto& entry: channel.History) {
            if (entry.GroupID != Max<ui32>()) {
                groups.insert(entry.GroupID);
            }
        }
    }

    for (ui32 group: groups) {
        const ui64 key = (static_cast<ui64>(nodeId) << 32) | group;
        with_lock (registry->Lock) {
            if (!registry->Keys.insert(key).second) {
                continue;
            }
        }

        const auto serviceId = MakeBlobStorageProxyID(group);
        const auto realProxy = actorSystem->LookupLocalService(serviceId);
        if (!realProxy) {
            with_lock (registry->Lock) {
                registry->Keys.erase(key);
            }
            LOG_WARN_S(ctx, TFileStoreComponents::TABLET,
                "[BSProxyInterceptor] group " << group
                << ": real proxy not registered yet, skipping");
            continue;
        }

        const auto interceptor = ctx.Register(
            new TBSProxyInterceptorActor(realProxy, group));
        actorSystem->RegisterLocalService(serviceId, interceptor);

        LOG_INFO_S(ctx, TFileStoreComponents::TABLET,
            "[BSProxyInterceptor] installed for group " << group
            << " service " << serviceId.ToString()
            << " interceptor " << interceptor.ToString()
            << " real proxy " << realProxy.ToString());
    }
}

}   // namespace NCloud::NFileStore::NStorage
