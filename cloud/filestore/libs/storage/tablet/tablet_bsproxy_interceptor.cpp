#include "tablet_bsproxy_interceptor.h"

#include <cloud/filestore/libs/storage/api/components.h>
#include <cloud/filestore/libs/storage/core/config.h>

#include <contrib/ydb/core/base/blobstorage.h>

#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/actors/core/log.h>

#include <util/generic/hash_set.h>
#include <util/random/fast.h>
#include <util/random/random.h>
#include <util/string/builder.h>
#include <util/system/guard.h>
#include <util/system/mutex.h>

#include <utility>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;
using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

double NormalizeProbability(double probability)
{
    if (probability < 0.0) {
        return 0.0;
    }

    if (probability > 100.0) {
        return 100.0;
    }

    return probability;
}

////////////////////////////////////////////////////////////////////////////////

class TBSProxyInterceptorActor final
    : public TActor<TBSProxyInterceptorActor>
{
private:
    const TActorId RealProxy;
    const ui32 GroupId;
    const double FailureProbability;
    const ui64 RandomFailureSeed;
    TFastRng64 Rng;
    const TString FailureErrorReason;

public:
    TBSProxyInterceptorActor(
            TActorId realProxy,
            ui32 groupId,
            TBSProxyInterceptorConfig config)
        : TActor(&TThis::StateWork)
        , RealProxy(realProxy)
        , GroupId(groupId)
        , FailureProbability(NormalizeProbability(config.FailureProbability) / 100.0F)
        , RandomFailureSeed(config.RandomFailureSeed)
        , Rng(RandomFailureSeed)
        , FailureErrorReason(TStringBuilder()
            << "injected by BSProxyInterceptor"
            << " group " << GroupId
            << " seed " << RandomFailureSeed)
    {}

private:
    bool ShouldInjectFailure()
    {
        if (FailureProbability <= 0.0) {
            return false;
        }

        if (FailureProbability >= 100.0) {
            return true;
        }

        return Rng.GenRandReal4() < FailureProbability;
    }

    template <typename TRequest>
    bool MaybeInjectFailure(
        TAutoPtr<IEventHandle>& ev,
        TRequest& request,
        const char* eventName)
    {
        if (!ShouldInjectFailure()) {
            return false;
        }

        auto response = request.MakeErrorResponse(
            NKikimrProto::ERROR,
            FailureErrorReason,
            TGroupId::FromValue(GroupId));
        response->ExecutionRelay = std::move(request.ExecutionRelay);

        LOG_WARN_S(*TlsActivationContext, TFileStoreComponents::TABLET,
            "[BSProxyInterceptor] group " << GroupId
            << " injecting " << eventName
            << " failure; not forwarding to " << RealProxy.ToString()
            << " sender " << ev->Sender.ToString()
            << " cookie " << ev->Cookie
            << " probabilityPercent " << FailureProbability
            << " seed " << RandomFailureSeed);

        Send(ev->Sender, response.release(), 0, ev->Cookie);
        return true;
    }

    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            case TEvents::TEvPoisonPill::EventType: {
                Die(TActivationContext::AsActorContext());
                return;
            }

            case TEvBlobStorage::EvPut: {
                auto* msg = ev->Get<TEvBlobStorage::TEvPut>();
                LOG_INFO_S(*TlsActivationContext, TFileStoreComponents::TABLET,
                    "[BSProxyInterceptor] group " << GroupId
                    << " TEvPut blob " << msg->Id.ToString()
                    << " channel " << msg->Id.Channel()
                    << " size " << msg->Id.BlobSize()
                    << " sender " << ev->Sender.ToString());
                if (MaybeInjectFailure(ev, *msg, "TEvPut")) {
                    return;
                }
                break;
            }

            case TEvBlobStorage::EvGet: {
                auto* msg = ev->Get<TEvBlobStorage::TEvGet>();
                LOG_INFO_S(*TlsActivationContext, TFileStoreComponents::TABLET,
                    "[BSProxyInterceptor] group " << GroupId
                    << " TEvGet queries " << msg->QuerySize
                    << " first " << (msg->QuerySize
                        ? msg->Queries[0].Id.ToString()
                        : TString("none"))
                    << " indexOnly " << msg->IsIndexOnly
                    << " sender " << ev->Sender.ToString());
                if (MaybeInjectFailure(ev, *msg, "TEvGet")) {
                    return;
                }
                break;
            }

            case TEvBlobStorage::EvRange: {
                auto* msg = ev->Get<TEvBlobStorage::TEvRange>();
                LOG_INFO_S(*TlsActivationContext, TFileStoreComponents::TABLET,
                    "[BSProxyInterceptor] group " << GroupId
                    << " TEvRange tablet " << msg->TabletId
                    << " from " << msg->From.ToString()
                    << " to " << msg->To.ToString()
                    << " sender " << ev->Sender.ToString());
                if (MaybeInjectFailure(ev, *msg, "TEvRange")) {
                    return;
                }
                break;
            }

            case TEvBlobStorage::EvCollectGarbage: {
                auto* msg = ev->Get<TEvBlobStorage::TEvCollectGarbage>();
                LOG_INFO_S(*TlsActivationContext, TFileStoreComponents::TABLET,
                    "[BSProxyInterceptor] group " << GroupId
                    << " TEvCollectGarbage tablet " << msg->TabletId
                    << " channel " << msg->Channel
                    << " gen " << msg->RecordGeneration
                    << " sender " << ev->Sender.ToString());
                if (MaybeInjectFailure(ev, *msg, "TEvCollectGarbage")) {
                    return;
                }
                break;
            }

            case TEvBlobStorage::EvBlock: {
                auto* msg = ev->Get<TEvBlobStorage::TEvBlock>();
                LOG_INFO_S(*TlsActivationContext, TFileStoreComponents::TABLET,
                    "[BSProxyInterceptor] group " << GroupId
                    << " TEvBlock tablet " << msg->TabletId
                    << " gen " << msg->Generation
                    << " sender " << ev->Sender.ToString());
                if (MaybeInjectFailure(ev, *msg, "TEvBlock")) {
                    return;
                }
                break;
            }

            case TEvBlobStorage::EvDiscover: {
                auto* msg = ev->Get<TEvBlobStorage::TEvDiscover>();
                LOG_INFO_S(*TlsActivationContext, TFileStoreComponents::TABLET,
                    "[BSProxyInterceptor] group " << GroupId
                    << " TEvDiscover tablet " << msg->TabletId
                    << " sender " << ev->Sender.ToString());
                if (MaybeInjectFailure(ev, *msg, "TEvDiscover")) {
                    return;
                }
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

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TBSProxyInterceptorConfig::TBSProxyInterceptorConfig(const TStorageConfig& config)
    : RandomFailuresEnabled(config.GetFakeBSProxyFailuresEnabled())
    , FailureProbability(config.GetFakeBSProxyFailuresProbabilityPercentage())
    , RandomFailureSeed(RandomNumber<ui64>())
{
}

void InstallBSProxyInterceptors(
    const TActorContext& ctx,
    const TTabletStorageInfo& info,
    const TBSProxyInterceptorConfig& config)
{
    auto* actorSystem = TActivationContext::ActorSystem();
    const ui32 nodeId = ctx.SelfID.NodeId();

    static TMutex interceptedGroupsLock;
    static THashSet<ui64> interceptedGroups;

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
        bool inserted = false;
        with_lock (interceptedGroupsLock) {
            inserted = interceptedGroups.insert(key).second;
        }
        if (!inserted) {
            LOG_INFO_S(ctx, TFileStoreComponents::TABLET,
                "[BSProxyInterceptor] group " << group << " already installed");
            continue;
        }

        const auto serviceId = MakeBlobStorageProxyID(group);
        const auto realProxy = actorSystem->LookupLocalService(serviceId);
        if (!realProxy) {
            with_lock (interceptedGroupsLock) {
                interceptedGroups.erase(key);
            }
            LOG_WARN_S(ctx, TFileStoreComponents::TABLET,
                "[BSProxyInterceptor] group " << group << ": real proxy not registered yet, skipping");
            continue;
        }

        const auto interceptor = ctx.Register(new TBSProxyInterceptorActor(realProxy, group, config));

        const auto previousProxy = actorSystem->RegisterLocalService(serviceId, interceptor);
        if (previousProxy != realProxy) {
            LOG_WARN_S(ctx, TFileStoreComponents::TABLET,
                "[BSProxyInterceptor] group " << group
                << " service " << serviceId.ToString()
                << " changed during installation: expected "
                << realProxy.ToString()
                << " previous " << previousProxy.ToString());
        }

        LOG_INFO_S(ctx, TFileStoreComponents::TABLET,
            "[BSProxyInterceptor] installed for group " << group
            << " service " << serviceId.ToString()
            << " interceptor " << interceptor.ToString()
            << " real proxy " << realProxy.ToString()
            << " probabilityPercent " << config.FailureProbability
            << " seed " << config.RandomFailureSeed);
    }
}

}   // namespace NCloud::NFileStore::NStorage
