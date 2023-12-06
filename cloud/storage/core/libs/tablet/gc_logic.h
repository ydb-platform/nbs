#pragma once

#include "public.h"

#include "blob_id.h"

#include <cloud/storage/core/libs/common/verify.h>

#include <ydb/core/base/blobstorage.h>

#include <ydb/library/actors/core/actor.h>

#include <util/generic/map.h>
#include <util/generic/vector.h>

namespace NCloud::NStorage {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
class TChannelRequests
{
    using TRequestMap = TMap<NActors::TActorId, T>;

private:
    ui64 TabletId;
    THashMap<ui32, TRequestMap> RequestsPerChannel;

public:
    TChannelRequests(const ui64 tabletId, const TVector<ui32>& channels)
        : TabletId(tabletId)
    {
        for (ui32 channel: channels) {
            RequestsPerChannel[channel] = {};
        }
    }

    TRequestMap& GetRequests(ui32 channel)
    {
        STORAGE_VERIFY(
            RequestsPerChannel.contains(channel),
            TWellKnownEntityTypes::TABLET,
            TabletId);
        return RequestsPerChannel[channel];
    }

    T& Get(
        const NKikimr::TTabletStorageInfo& info,
        const NKikimr::TLogoBlobID& blobId)
    {
        return Get(info, blobId.Channel(), blobId.Generation());
    }

    T& Get(
        const NKikimr::TTabletStorageInfo& info,
        const TPartialBlobId& blobId)
    {
        return Get(info, blobId.Channel(), blobId.Generation());
    }

    T& Get(
        const NKikimr::TTabletStorageInfo& info,
        ui32 channel,
        ui32 generation)
    {
        auto proxyId = info.BSProxyIDForChannel(channel, generation);

        auto& requests = GetRequests(channel);
        return requests[proxyId];
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TCollectBarrier
{
    ui64 CollectCommitId = 0;
};

using TCollectBarriers = TChannelRequests<TCollectBarrier>;

////////////////////////////////////////////////////////////////////////////////

struct TCollectRequest
{
    std::unique_ptr<TVector<NKikimr::TLogoBlobID>> Keep;
    std::unique_ptr<TVector<NKikimr::TLogoBlobID>> DoNotKeep;

    TCollectRequest()
        : Keep(new TVector<NKikimr::TLogoBlobID>())
        , DoNotKeep(new TVector<NKikimr::TLogoBlobID>())
    {}
};

using TCollectRequests = TChannelRequests<TCollectRequest>;

////////////////////////////////////////////////////////////////////////////////

TCollectBarriers BuildGCBarriers(
    const NKikimr::TTabletStorageInfo& tabletInfo,
    const TVector<ui32>& channels,
    const TVector<TPartialBlobId>& knownBlobs,
    ui64 collectCommitId);

TCollectRequests BuildGCRequests(
    const NKikimr::TTabletStorageInfo& tabletInfo,
    const TVector<ui32>& channels,
    const TVector<TPartialBlobId>& newBlobs,
    const TVector<TPartialBlobId>& garbageBlobs,
    bool cleanupHistory,
    ui64 lastGCCommitId,
    ui64 collectCommitId,
    ui32 collectCounter);

void RemoveDuplicates(
    TVector<TPartialBlobId>& newBlobs,
    TVector<TPartialBlobId>& garbageBlobs,
    ui64 commitId);

void FindGarbageVersions(
    const TVector<ui64>& checkpoints,
    const TVector<ui64>& commitIds,
    TVector<ui64>& garbage);

}   // namespace NCloud::NStorage
