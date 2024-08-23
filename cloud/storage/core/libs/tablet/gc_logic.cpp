#include "gc_logic.h"

#include <cloud/storage/core/libs/tablet/model/commit.h>

#include <util/generic/algorithm.h>
#include <util/stream/str.h>
#include <util/string/printf.h>

namespace NCloud::NStorage {

using namespace NActors;

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

TCollectBarriers BuildGCBarriers(
    const TTabletStorageInfo& tabletInfo,
    const TVector<ui32>& channels,
    const TVector<TPartialBlobId>& knownBlobs,
    ui64 collectCommitId)
{
    TCollectBarriers requests(tabletInfo.TabletID, channels);

    // set barriers for all known groups
    for (ui32 channel: channels) {
        const auto* channelInfo = tabletInfo.ChannelInfo(channel);
        STORAGE_VERIFY(
            channelInfo,
            TWellKnownEntityTypes::TABLET,
            tabletInfo.TabletID);

        for (const auto& entry: channelInfo->History) {
            auto& request = requests.Get(
                tabletInfo,
                channel,
                entry.FromGeneration);

            request.CollectCommitId = collectCommitId;
        }
    }

    // lower barriers to min(gen:step) for all known blobs
    for (const auto& blobId: knownBlobs) {
        auto& request = requests.Get(tabletInfo, blobId);

        if (request.CollectCommitId >= blobId.CommitId()) {
            request.CollectCommitId = blobId.CommitId() - 1;
        }
    }

    return requests;
}

TCollectRequests BuildGCRequests(
    const TTabletStorageInfo& tabletInfo,
    const TVector<ui32>& channels,
    const TVector<TPartialBlobId>& newBlobs,
    const TVector<TPartialBlobId>& garbageBlobs,
    bool cleanupHistory,
    ui64 lastGCCommitId,
    ui64 collectCommitId,
    ui32 collectCounter)
{
    TCollectRequests requests(tabletInfo.TabletID, channels);
    TVector<TLogoBlobID> allBlobIds;

    auto lastGC  = ParseCommitId(lastGCCommitId);
    auto collect = ParseCommitId(collectCommitId);

    for (const auto& partialBlobId: newBlobs) {
        STORAGE_VERIFY(
            !IsDeletionMarker(partialBlobId),
            TWellKnownEntityTypes::TABLET,
            tabletInfo.TabletID);

        auto blobId = MakeBlobId(tabletInfo.TabletID, partialBlobId);
        allBlobIds.push_back(blobId);

        auto& request = requests.Get(tabletInfo, blobId);
        request.Keep->push_back(blobId);
    }

    for (const auto& partialBlobId: garbageBlobs) {
        STORAGE_VERIFY(
            !IsDeletionMarker(partialBlobId),
            TWellKnownEntityTypes::TABLET,
            tabletInfo.TabletID);

        auto blobId = MakeBlobId(tabletInfo.TabletID, partialBlobId);
        allBlobIds.push_back(blobId);

        auto& request = requests.Get(tabletInfo, blobId);
        request.DoNotKeep->push_back(blobId);
    }

    if (allBlobIds.size() > 1) {
        // ensure there are no duplicates
        Sort(allBlobIds);

        auto blobId = allBlobIds[0];
        for (ui32 i = 1; i < allBlobIds.size(); ++i) {
            STORAGE_VERIFY_C(
                blobId != allBlobIds[i],
                TWellKnownEntityTypes::TABLET,
                tabletInfo.TabletID,
                Sprintf(
                    "[%lu] Duplicated blob %s detected in CollectGarbage request: "
                    "record=[%u:%u], collect=[%u:%u], keep={%s}, delete={%s}",
                    tabletInfo.TabletID,
                    ToString(blobId).data(),
                    collect.first, collectCounter,
                    collect.first, collect.second,
                    DumpBlobIds(tabletInfo.TabletID, newBlobs).data(),
                    DumpBlobIds(tabletInfo.TabletID, garbageBlobs).data()));

            blobId = allBlobIds[i];
        }
    }

    if (lastGC.first != collect.first) {
        for (ui32 channel: channels) {
            const auto* channelInfo = tabletInfo.ChannelInfo(channel);

            auto begin =  channelInfo->History.begin();
            auto end = channelInfo->History.end();
            auto it = begin;

            if (!cleanupHistory) {
                it = UpperBound(
                    begin,
                    end,
                    lastGC.first,
                    [] (ui32 value, const auto& item) { return value < item.FromGeneration; });
                if (it != begin) {
                    --it;
                }
            }

            for (; it != end; ++it) {
                requests.Get(tabletInfo, channel, it->FromGeneration);
            }
        }
    }

    return requests;
}

void RemoveDuplicates(
    TVector<TPartialBlobId>& newBlobs,
    TVector<TPartialBlobId>& garbageBlobs,
    ui64 commitId)
{
    auto genstep = ParseCommitId(commitId);

    auto nit = newBlobs.begin(); auto nend = newBlobs.end();
    auto git = garbageBlobs.begin(); auto gend = garbageBlobs.end();

    while (nit != nend && git != gend) {
        if (*nit < *git) {
            ++nit;
        } else if (*nit > *git) {
            ++git;
        } else {
            // if blob appeared as new in previous generation
            // we should leave it in garbageBlobs to be sure it is
            // finally collected
            if (nit->Generation() == genstep.first) {
                *git = InvalidPartialBlobId;
                ++git;
            }
            *nit = InvalidPartialBlobId;
            ++nit;
        }
    }

    auto it =
        std::remove(newBlobs.begin(), newBlobs.end(), InvalidPartialBlobId);
    newBlobs.erase(it, newBlobs.end());

    it = std::remove(
        garbageBlobs.begin(),
        garbageBlobs.end(),
        InvalidPartialBlobId);
    garbageBlobs.erase(it, garbageBlobs.end());
}

void FindGarbageVersions(
    const TVector<ui64>& checkpoints,
    const TVector<ui64>& commitIds,
    TVector<ui64>& garbage)
{
    Y_ASSERT(IsSorted(checkpoints.begin(), checkpoints.end(), TGreater<ui64>()));
    Y_ASSERT(IsSorted(commitIds.begin(), commitIds.end(), TGreater<ui64>()));

    if (!checkpoints) {
        garbage.insert(garbage.end(), commitIds.begin(), commitIds.end());
        return;
    }

    if (!commitIds) {
        return;
    }

    if (commitIds.size() == 1) {
        if (commitIds.front() > checkpoints.front()) {
            garbage.push_back(commitIds.front());
        }
        return;
    }

    auto lit = checkpoints.begin();
    auto lend = checkpoints.end();

    auto rit = commitIds.begin();
    auto rend = commitIds.end();

    bool covered = false;
    for (;;) {
        if (*lit < *rit) {
            if (!covered) {
                garbage.push_back(*rit);
            } else {
                covered = false;
            }

            if (++rit == rend) {
                break;
            }
        } else {
            covered = true;

            if (++lit == lend) {
                ++rit;
                while (rit != rend) {
                    garbage.push_back(*rit);
                    ++rit;
                }
                break;
            }
        }
    }
}

}   // namespace NCloud::NStorage
