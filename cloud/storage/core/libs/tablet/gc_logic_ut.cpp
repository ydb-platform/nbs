#include "gc_logic.h"

#include <cloud/storage/core/libs/kikimr/public.h>
#include <cloud/storage/core/libs/tablet/model/commit.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NStorage {

using namespace NActors;

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

void SetupTabletInfo(TTabletStorageInfo& info, ui32 nchannels)
{
    info.Channels = TVector<TTabletChannelInfo>(nchannels);
}

void AddNewHistoryToTabletInfo(TTabletStorageInfo& info, ui32 newGeneration, ui32 newGroup)
{
    for (ui64 channel = 0; channel < info.Channels.size(); ++channel) {
        info.Channels[channel].History.push_back({newGeneration, newGroup});
    }
}

void AddNewHistoryToTabletInfo(
    TTabletStorageInfo& info,
    std::pair<ui64, ui64> channels,
    ui32 newGeneration,
    ui32 newGroup)
{
    UNIT_ASSERT(channels.second < info.Channels.size());
    for (ui64 channel = channels.first; channel <= channels.second; ++channel) {
        info.Channels[channel].History.push_back({newGeneration, newGroup});
    }
}

void AddNewChannel(TTabletStorageInfo& info, ui32 generation, ui32 group)
{
    info.Channels.push_back(TTabletChannelInfo());
    info.Channels.back().History.push_back({generation, group});
}

template <typename T>
void AssertEqual(const TVector<T>& l, const TVector<T>& r)
{
    UNIT_ASSERT_VALUES_EQUAL(l.size(), r.size());
    for (size_t i = 0; i < l.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL(l[i], r[i]);
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TGCLogicTest)
{
    Y_UNIT_TEST(ShouldStartGConGroupsNoLongerActiveIfTabletGenerationChanged)
    {
        TTabletStorageInfoPtr tabletInfo = MakeIntrusive<TTabletStorageInfo>();

        SetupTabletInfo(*tabletInfo, 4);

        AddNewHistoryToTabletInfo(*tabletInfo, 0 , 0);
        AddNewHistoryToTabletInfo(*tabletInfo, 3 , 1);

        TVector<ui32> channels = {3};

        auto response = BuildGCRequests(
            *tabletInfo,
            channels,
            { TPartialBlobId(3, 10, 3, 4*1024*1024, 0, 0) },
            TVector<TPartialBlobId>(),
            false,
            MakeCommitId(0, 1000),
            MakeCommitId(3, 1000),
            0);

        ui32 NumberOfGroupsSeen = 0;
        ui32 NumberOfNewBlobs = 0;
        ui32 NumberOfGarbageBlobs = 0;

        for (ui32 ch: channels) {
            NumberOfGroupsSeen += response.GetRequests(ch).size();
            for (const auto& element: response.GetRequests(ch)) {
                NumberOfNewBlobs += element.second.Keep->size();
                NumberOfGarbageBlobs += element.second.DoNotKeep->size();
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(NumberOfGroupsSeen, 2);
        UNIT_ASSERT_VALUES_EQUAL(NumberOfNewBlobs, 1);
        UNIT_ASSERT_VALUES_EQUAL(NumberOfGarbageBlobs, 0);
    }

    Y_UNIT_TEST(ShouldNotStartGcOnExtraGroups)
    {
        TTabletStorageInfoPtr tabletInfo = MakeIntrusive<TTabletStorageInfo>();

        SetupTabletInfo(*tabletInfo, 4);
        AddNewHistoryToTabletInfo(*tabletInfo, 0 , 0);
        AddNewHistoryToTabletInfo(*tabletInfo, 2 , 1);
        AddNewHistoryToTabletInfo(*tabletInfo, 3 , 2);

        TVector<ui32> channels = {3};

        auto response = BuildGCRequests(
            *tabletInfo,
            channels,
            { TPartialBlobId(3, 10, 3, 4*1024*1024, 0, 0) },
            TVector<TPartialBlobId>(),
            false,
            MakeCommitId(2, 1000),
            MakeCommitId(3, 1000),
            0);

        ui32 NumberOfGroupsSeen = 0;
        ui32 NumberOfNewBlobs = 0;
        ui32 NumberOfGarbageBlobs = 0;
        bool GroupZeroIsSeen = false;

        for (ui32 ch: channels) {
            auto& requests = response.GetRequests(ch);
            NumberOfGroupsSeen += requests.size();
            if (requests.contains(tabletInfo->BSProxyIDForChannel(ch, 0))) {
                GroupZeroIsSeen = true;
            }
            for (const auto& element: response.GetRequests(ch)) {
                NumberOfNewBlobs += element.second.Keep->size();
                NumberOfGarbageBlobs += element.second.DoNotKeep->size();
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(GroupZeroIsSeen, false);
        UNIT_ASSERT_VALUES_EQUAL(NumberOfGroupsSeen, 2);
        UNIT_ASSERT_VALUES_EQUAL(NumberOfNewBlobs, 1);
        UNIT_ASSERT_VALUES_EQUAL(NumberOfGarbageBlobs, 0);
    }

    Y_UNIT_TEST(ShouldNotCollectGroupsNoLongerActiveIfTabletGenerationHasNotChanged)
    {
        TTabletStorageInfoPtr tabletInfo = MakeIntrusive<TTabletStorageInfo>();

        SetupTabletInfo(*tabletInfo, 4);
        AddNewHistoryToTabletInfo(*tabletInfo, 0 , 0);
        AddNewHistoryToTabletInfo(*tabletInfo, 3 , 1);

        TVector<ui32> channels = {3};

        auto response = BuildGCRequests(
            *tabletInfo,
            channels,
            { TPartialBlobId(3, 10, 3, 4*1024*1024, 0, 0) },
            TVector<TPartialBlobId>(),
            false,
            MakeCommitId(3, 100),
            MakeCommitId(3, 1000),
            0);

        ui32 NumberOfGroupsSeen = 0;
        ui32 NumberOfNewBlobs = 0;
        ui32 NumberOfGarbageBlobs = 0;
        bool GroupZeroIsSeen = false;

        for (ui32 ch: channels) {
            auto& requests = response.GetRequests(ch);
            NumberOfGroupsSeen += requests.size();
            if (requests.contains(tabletInfo->BSProxyIDForChannel(ch, 0))) {
                GroupZeroIsSeen = true;
            }
            for (const auto& element: response.GetRequests(ch)) {
                NumberOfNewBlobs += element.second.Keep->size();
                NumberOfGarbageBlobs += element.second.DoNotKeep->size();
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(GroupZeroIsSeen, false);
        UNIT_ASSERT_VALUES_EQUAL(NumberOfGroupsSeen, 1);
        UNIT_ASSERT_VALUES_EQUAL(NumberOfNewBlobs, 1);
        UNIT_ASSERT_VALUES_EQUAL(NumberOfGarbageBlobs, 0);
    }

    Y_UNIT_TEST(ShouldKeepVersionsCoveredByCheckpoint)
    {
        {
            TVector<ui64> checkpoints = {};
            TVector<ui64> commitIds = { 351, 350, 151, 150, 91, 90 };

            TVector<ui64> result;
            FindGarbageVersions(checkpoints, commitIds, result);

            AssertEqual(result, { 351, 350, 151, 150, 91, 90 });
        }

        {
            TVector<ui64> checkpoints = { 300, 200, 100};
            TVector<ui64> commitIds = { 350 };

            TVector<ui64> result;
            FindGarbageVersions(checkpoints, commitIds, result);

            AssertEqual(result, { 350 });
        }

        {
            TVector<ui64> checkpoints = { 300, 200, 100};
            TVector<ui64> commitIds = { 250 };

            TVector<ui64> result;
            FindGarbageVersions(checkpoints, commitIds, result);

            AssertEqual(result, {});
        }

        {
            TVector<ui64> checkpoints = { 300, 200, 100 };
            TVector<ui64> commitIds = { 351, 350, 151, 150, 91, 90 };

            TVector<ui64> result;
            FindGarbageVersions(checkpoints, commitIds, result);

            AssertEqual(result, { 351, 350, 150, 90 });
        }
    }

    Y_UNIT_TEST(ShouldHandleNewChannel)
    {
        TTabletStorageInfoPtr tabletInfo = MakeIntrusive<TTabletStorageInfo>();

        SetupTabletInfo(*tabletInfo, 4);
        AddNewHistoryToTabletInfo(*tabletInfo, 0 , 0);
        AddNewHistoryToTabletInfo(*tabletInfo, 3 , 1);
        AddNewChannel(*tabletInfo, 5, 2);

        TVector<ui32> channels = {3, 4};

        auto response = BuildGCRequests(
            *tabletInfo,
            channels,
            { TPartialBlobId(3, 10, 3, 4*1024*1024, 0, 0) },
            TVector<TPartialBlobId>(),
            false,
            MakeCommitId(3, 100),
            MakeCommitId(5, 1000),
            0);
        UNIT_ASSERT_VALUES_EQUAL(response.GetRequests(4).size(), 1);
    }

    Y_UNIT_TEST(ShouldGConNonActiveGroupsIfRequested)
    {
        TTabletStorageInfoPtr tabletInfo = MakeIntrusive<TTabletStorageInfo>();

        SetupTabletInfo(*tabletInfo, 4);

        AddNewHistoryToTabletInfo(*tabletInfo, std::make_pair(3, 3), 0 , 0);
        AddNewHistoryToTabletInfo(*tabletInfo, std::make_pair(3, 3), 1 , 1);
        AddNewHistoryToTabletInfo(*tabletInfo, std::make_pair(3, 3), 4 , 2);
        AddNewHistoryToTabletInfo(*tabletInfo, std::make_pair(3, 3), 6 , 3);

        TVector<ui32> channels = {3};

        auto response = BuildGCRequests(
            *tabletInfo,
            channels,
            { TPartialBlobId(3, 10, 3, 4*1024*1024, 0, 0) },
            TVector<TPartialBlobId>(),
            false,
            MakeCommitId(2, 1000),
            MakeCommitId(6, 1000),
            0);

        ui32 NumberOfGroupsSeen = 0;
        ui32 NumberOfNewBlobs = 0;
        ui32 NumberOfGarbageBlobs = 0;

        for (ui32 ch: channels) {
            NumberOfGroupsSeen += response.GetRequests(ch).size();
            for (const auto& element: response.GetRequests(ch)) {
                NumberOfNewBlobs += element.second.Keep->size();
                NumberOfGarbageBlobs += element.second.DoNotKeep->size();
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(NumberOfGroupsSeen, 3);
        UNIT_ASSERT_VALUES_EQUAL(NumberOfNewBlobs, 1);
        UNIT_ASSERT_VALUES_EQUAL(NumberOfGarbageBlobs, 0);

        response = BuildGCRequests(
            *tabletInfo,
            channels,
            { TPartialBlobId(3, 10, 3, 4*1024*1024, 0, 0) },
            TVector<TPartialBlobId>(),
            true,
            MakeCommitId(2, 1000),
            MakeCommitId(6, 1000),
            0);

        NumberOfGroupsSeen = 0;
        NumberOfNewBlobs = 0;
        NumberOfGarbageBlobs = 0;

        for (ui32 ch: channels) {
            NumberOfGroupsSeen += response.GetRequests(ch).size();
            for (const auto& element: response.GetRequests(ch)) {
                NumberOfNewBlobs += element.second.Keep->size();
                NumberOfGarbageBlobs += element.second.DoNotKeep->size();
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(NumberOfGroupsSeen, 4);
        UNIT_ASSERT_VALUES_EQUAL(NumberOfNewBlobs, 1);
        UNIT_ASSERT_VALUES_EQUAL(NumberOfGarbageBlobs, 0);
    }

    Y_UNIT_TEST(ShouldPeekLastChannelHistoryElementIfItHoldsLastGcBarrier)
    {
        TTabletStorageInfoPtr tabletInfo = MakeIntrusive<TTabletStorageInfo>();

        SetupTabletInfo(*tabletInfo, 4);

        AddNewHistoryToTabletInfo(*tabletInfo, std::make_pair(3, 3), 0 , 0);
        AddNewHistoryToTabletInfo(*tabletInfo, std::make_pair(3, 3), 1 , 1);
        AddNewHistoryToTabletInfo(*tabletInfo, std::make_pair(3, 3), 4 , 2);
        AddNewHistoryToTabletInfo(*tabletInfo, std::make_pair(3, 3), 6 , 3);

        TVector<ui32> channels = {3};

        auto response = BuildGCRequests(
            *tabletInfo,
            channels,
            TVector<TPartialBlobId>(),
            TVector<TPartialBlobId>(),
            false,
            MakeCommitId(7, 1000),
            MakeCommitId(8, 1000),
            0);

        ui32 NumberOfGroupsSeen = 0;
        ui32 NumberOfNewBlobs = 0;
        ui32 NumberOfGarbageBlobs = 0;

        for (ui32 ch: channels) {
            NumberOfGroupsSeen += response.GetRequests(ch).size();
            for (const auto& element: response.GetRequests(ch)) {
                NumberOfNewBlobs += element.second.Keep->size();
                NumberOfGarbageBlobs += element.second.DoNotKeep->size();
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(NumberOfGroupsSeen, 1);
        UNIT_ASSERT_VALUES_EQUAL(NumberOfNewBlobs, 0);
        UNIT_ASSERT_VALUES_EQUAL(NumberOfGarbageBlobs, 0);
    }

    Y_UNIT_TEST(ShouldRemoveDuplicates)
    {
        auto blob1 = TPartialBlobId(2, 10, 3, 4*1024*1024, 0, 0);
        auto blob2 = TPartialBlobId(3, 10, 2, 4*1024*1024, 0, 0);
        auto blob3 = TPartialBlobId(3, 10, 4, 4*1024*1024, 0, 0);
        auto blob4 = TPartialBlobId(3, 11, 6, 4*1024*1024, 0, 0);

        TVector<TPartialBlobId> newBlobs = {
            // previous generation, should only be present in garbageBlobs
            blob1,
            // present in both newBlobs and garbageBlobs, should be excluded from both
            blob2,
            // present only in newBlobs, should be kept
            blob3,
        };

        TVector<TPartialBlobId> garbageBlobs = {
            // previous generation, should only be present in garbageBlobs
            blob1,
            // present in both newBlobs and garbageBlobs, should be excluded from both
            blob2,
            // present only in garbageBlobs, should be kept
            blob4,
        };

        RemoveDuplicates(newBlobs, garbageBlobs, MakeCommitId(3, 0));
        UNIT_ASSERT_VALUES_EQUAL(newBlobs.size(), 1);
        UNIT_ASSERT_EQUAL(newBlobs[0], blob3);

        UNIT_ASSERT_VALUES_EQUAL(garbageBlobs.size(), 2);
        UNIT_ASSERT_EQUAL(garbageBlobs[0], blob1);
        UNIT_ASSERT_EQUAL(garbageBlobs[1], blob4);
    }
}

}   // namespace NCloud::NStorage
