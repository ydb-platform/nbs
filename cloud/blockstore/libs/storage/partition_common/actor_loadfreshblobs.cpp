#include "actor_loadfreshblobs.h"

#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/kikimr/helpers.h>
#include <cloud/storage/core/libs/kikimr/helpers.h>

#include <cloud/storage/core/libs/common/verify.h>
#include <cloud/storage/core/libs/tablet/model/commit.h>
#include <cloud/storage/core/libs/tablet/model/partial_blob_id.h>

#include <util/generic/size_literals.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

TLoadFreshBlobsActor::TLoadFreshBlobsActor(
        const TActorId& partitionActorId,
        TTabletStorageInfoPtr tabletInfo,
        EStorageAccessMode storageAccessMode,
        ui64 trimFreshLogToCommitId,
        TVector<ui32> freshChannels)
    : PartitionActorId(partitionActorId)
    , TabletInfo(std::move(tabletInfo))
    , StorageAccessMode(storageAccessMode)
    , TrimFreshLogToCommitId(trimFreshLogToCommitId)
    , FreshChannels(std::move(freshChannels))
{
    Y_UNUSED(StorageAccessMode);
}

void TLoadFreshBlobsActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    if (FreshChannels.empty()) {
        LOG_WARN(ctx, TBlockStoreComponents::PARTITION,
            "[%lu] TLoadFreshBlobsActor: no fresh channels",
            TabletInfo->TabletID);

        NotifyAndDie(ctx);
        return;
    }

    DiscoverBlobs(ctx);
}

void TLoadFreshBlobsActor::DiscoverBlobs(const TActorContext& ctx)
{
    const auto tabletId = TabletInfo->TabletID;

    auto [barrierGen, barrierStep] = ParseCommitId(TrimFreshLogToCommitId);

    for (ui32 channel: FreshChannels) {
        LOG_INFO(ctx, TBlockStoreComponents::PARTITION,
            "[%lu] TLoadFreshBlobsActor: loading fresh blobs from channel %u",
            TabletInfo->TabletID,
            channel);

        const auto* channelInfo = TabletInfo->ChannelInfo(channel);
        Y_ABORT_UNLESS(channelInfo);

        auto begin = channelInfo->History.begin();
        auto end = channelInfo->History.end();
        if (begin == end) {
            Error = MakeError(E_FAIL, TStringBuilder() <<
                "empty history for fresh channel " << channel);
            NotifyAndDie(ctx);
            return;
        }

        const auto historyVsHistory = [] (const auto& l, const auto& r) {
            return l.FromGeneration < r.FromGeneration;
        };

        const auto genVsHistory = [] (const ui64 l, const auto& r) {
            return l < r.FromGeneration;
        };

        STORAGE_VERIFY(
            IsSorted(begin, end, historyVsHistory),
            TWellKnownEntityTypes::TABLET,
            TabletInfo->TabletID);

        auto cur = begin;
        if (barrierGen != 0) {
            cur = std::upper_bound(begin, end, barrierGen, genVsHistory);
            STORAGE_VERIFY(
                cur != begin,
                TWellKnownEntityTypes::TABLET,
                TabletInfo->TabletID);

            --cur;
        }
        auto next = std::next(cur);

        if (cur != begin) {
            LOG_INFO(ctx, TBlockStoreComponents::PARTITION,
                "[%lu] TLoadFreshBlobsActor: skipping %u history groups"
                ", first FromGeneration=%u",
                TabletInfo->TabletID,
                std::distance(begin, cur),
                cur->FromGeneration);
        }

        for (;;) {
            const ui32 fromGen = barrierGen;
            const ui32 fromStep = barrierStep;

            const ui32 toGen = (next == end ? Max<ui32>() : next->FromGeneration);
            const ui32 toStep = Max<ui32>();

            NKikimr::TLogoBlobID fromId(
                tabletId,
                fromGen,
                fromStep,
                channel,
                0,   // min blob size
                0);  // min cookie

            NKikimr::TLogoBlobID toId(
                tabletId,
                toGen,
                toStep,
                channel,
                NKikimr::TLogoBlobID::MaxBlobSize,
                NKikimr::TLogoBlobID::MaxCookie);

            LOG_INFO(ctx, TBlockStoreComponents::PARTITION,
                "[%lu] TLoadFreshBlobsActor: sending EvRange %u:%u, %u:%u"
                " to group %u",
                TabletInfo->TabletID,
                fromGen, fromStep,
                toGen, toStep,
                cur->GroupID);

            auto request = std::make_unique<TEvBlobStorage::TEvRange>(
                tabletId,
                fromId,
                toId,
                true,               // restore
                TInstant::Max(),    // deadline
                false);             // indexOnly

            SendToBSProxy(
                ctx,
                cur->GroupID,
                request.release(),
                RangeRequestsInFlight++);  // cookie

            if (next == end) {
                break;
            }

            ++cur;
            ++next;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

void TLoadFreshBlobsActor::NotifyAndDie(const TActorContext& ctx)
{
    using TEvent = TEvPartitionCommonPrivate::TEvLoadFreshBlobsCompleted;
    auto ev = std::make_unique<TEvent>(
        std::move(Error),
        std::move(Blobs));

    NCloud::Send(ctx, PartitionActorId, std::move(ev));

    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TLoadFreshBlobsActor::HandleRangeResult(
    const TEvBlobStorage::TEvRangeResult::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    ui64 totalBlobSize = 0;
    ui64 freshBlobCount = 0;

    auto error = MakeKikimrError(msg->Status, msg->ErrorReason);
    if (HasError(error)) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::PARTITION,
            "[%lu] Fresh blobs range request failed: %s",
            TabletInfo->TabletID,
            FormatError(error).c_str());

        Error = std::move(error);
    } else {
        for (const auto& r: msg->Responses) {
            const ui64 commitId = MakeCommitId(r.Id.Generation(), r.Id.Step());
            Blobs.emplace_back(commitId, std::move(r.Buffer));

            totalBlobSize += r.Id.BlobSize();
            ++freshBlobCount;
        }
    }

    Y_ABORT_UNLESS(RangeRequestsInFlight > 0);
    if (--RangeRequestsInFlight > 0) {
        return;
    }

    if (!HasError(Error)) {
        LOG_INFO(ctx, TBlockStoreComponents::PARTITION,
            "[%lu] Read fresh blobs (blob count: %lu, total blob size: %lu)",
            TabletInfo->TabletID,
            freshBlobCount,
            totalBlobSize);
    }

    NotifyAndDie(ctx);
}

void TLoadFreshBlobsActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    Die(ctx);
}

STFUNC(TLoadFreshBlobsActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(TEvBlobStorage::TEvRangeResult, HandleRangeResult);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::PARTITION,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace NCloud::NBlockStore::NStorage
