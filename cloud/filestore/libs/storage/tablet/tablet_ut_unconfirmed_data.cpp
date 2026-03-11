#include "tablet_schema.h"
#include "tablet_tx.h"

#include <cloud/filestore/libs/service/request.h>
#include <cloud/filestore/libs/storage/testlib/tablet_client.h>
#include <cloud/filestore/libs/storage/testlib/test_env.h>

#include <contrib/ydb/core/base/blobstorage.h>
#include <contrib/ydb/core/base/logoblob.h>
#include <contrib/ydb/core/mind/local.h>
#include <contrib/ydb/core/testlib/basics/storage.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/random/fast.h>

#include <algorithm>
#include <initializer_list>

namespace NCloud::NFileStore::NStorage {

using namespace NKikimr;

namespace {

class TTestProfileLog: public IProfileLog
{
public:
    TMap<ui32, TVector<TRecord>> Requests;

    void Start() override
    {}

    void Stop() override
    {}

    void Write(TRecord record) override
    {
        UNIT_ASSERT(record.Request.HasRequestType());
        Requests[record.Request.GetRequestType()].push_back(std::move(record));
    }
};

////////////////////////////////////////////////////////////////////////////////

ui64 GenerateBlobIdsAndPutBlob(
    TTestEnv& env,
    const TEvIndexTablet::TEvGenerateBlobIdsResponse& gbi,
    TStringBuf data)
{
    UNIT_ASSERT_VALUES_EQUAL(1, gbi.Record.BlobsSize());

    const auto blobId =
        LogoBlobIDFromLogoBlobID(gbi.Record.GetBlobs(0).GetBlobId());
    const ui64 commitId = gbi.Record.GetCommitId();
    const ui32 bsGroupId = gbi.Record.GetBlobs(0).GetBSGroupId();

    const TString buffer(data);
    auto evPut = std::make_unique<TEvBlobStorage::TEvPut>(
        blobId,
        buffer,
        TInstant::Max(),
        NKikimrBlobStorage::UserData);

    const auto proxy = MakeBlobStorageProxyID(bsGroupId);
    auto evPutSender = env.GetRuntime().AllocateEdgeActor(proxy.NodeId());
    env.GetRuntime().Send(CreateEventForBSProxy(
        evPutSender,
        proxy,
        evPut.release(),
        blobId.Cookie()));
    env.GetRuntime().DispatchEvents({}, TDuration::MilliSeconds(100));

    return commitId;
}

ui64 GenerateBlobIdsAndPutBlob(
    TTestEnv& env,
    TIndexTabletClient& tablet,
    ui64 nodeId,
    ui64 handle,
    ui64 offset,
    ui32 blockSize,
    char fillChar)
{
    auto gbi = tablet.GenerateBlobIds(nodeId, handle, offset, blockSize);
    const TString data(blockSize, fillChar);
    return GenerateBlobIdsAndPutBlob(env, *gbi, data);
}

ui64 GenerateBlobIdsPutBlobAndConfirm(
    TTestEnv& env,
    TIndexTabletClient& tablet,
    const TEvIndexTablet::TEvGenerateBlobIdsResponse& gbi,
    TStringBuf data)
{
    const ui64 commitId = GenerateBlobIdsAndPutBlob(env, gbi, data);
    tablet.SendConfirmAddDataRequest(commitId);
    tablet.AssertConfirmAddDataResponse(S_OK);
    return commitId;
}

void WaitForTabletCommit(TTestEnv& env)
{
    NActors::TDispatchOptions options;
    options.FinalEvents.emplace_back(TEvTablet::EvCommitResult);
    env.GetRuntime().DispatchEvents(options, TDuration::MilliSeconds(1000));
}

ui64 GenerateBlobIdsPutBlobAndConfirm(
    TTestEnv& env,
    TIndexTabletClient& tablet,
    ui64 nodeId,
    ui64 handle,
    ui64 offset,
    ui32 blockSize,
    char fillChar)
{
    const auto statsBefore = GetStorageStats(tablet);
    const ui32 expectedUnconfirmedDataCount =
        statsBefore.GetUnconfirmedDataCount() + 1;

    auto gbi = tablet.GenerateBlobIds(nodeId, handle, offset, blockSize);
    const TString data(blockSize, fillChar);
    const ui64 commitId = GenerateBlobIdsAndPutBlob(env, *gbi, data);
    WaitForTabletCommit(env);
    const auto statsAfterGenerate = GetStorageStats(tablet);
    UNIT_ASSERT_VALUES_EQUAL(
        expectedUnconfirmedDataCount,
        statsAfterGenerate.GetUnconfirmedDataCount());
    tablet.SendConfirmAddDataRequest(commitId);
    tablet.AssertConfirmAddDataResponse(S_OK);
    return commitId;
}

void AssertStorageStats(
    TIndexTabletClient& tablet,
    ui32 expectedUnconfirmedDataCount,
    ui32 expectedConfirmedDataCount = 0)
{
    const auto stats = GetStorageStats(tablet);
    UNIT_ASSERT_VALUES_EQUAL(
        expectedUnconfirmedDataCount,
        stats.GetUnconfirmedDataCount());
    UNIT_ASSERT_VALUES_EQUAL(
        expectedConfirmedDataCount,
        stats.GetConfirmedDataCount());
}

struct TExpectedDataPart
{
    ui64 Length;
    char Fill;
};

TString BuildExpectedData(std::initializer_list<TExpectedDataPart> parts)
{
    TString data;
    for (const auto& part: parts) {
        data += TString(part.Length, part.Fill);
    }
    return data;
}

ui64 RebootTabletAndCreateHandle(TIndexTabletClient& tablet, ui64 nodeId)
{
    tablet.RebootTablet();
    tablet.InitSession("client", "session");
    return CreateHandle(tablet, nodeId);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TIndexTabletTest_UnconfirmedData)
{
    using namespace NActors;
    using namespace NCloud::NStorage;

    Y_UNIT_TEST(ShouldPreserveConfirmedDataRefCommitIdAcrossAddBlobClear)
    {
        constexpr ui64 confirmedCommitId = 42;

        TTxIndexTablet::TAddBlob args(
            {},   // requestInfo
            EAddBlobMode::Write,
            {},   // srcBlobs
            {},   // srcBlocks
            {},   // mixedBlobs
            {},   // mergedBlobs
            {},   // writeRanges
            {},   // unalignedDataParts
            confirmedCommitId);

        args.CommitId = 100;
        args.Clear();

        UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, args.CommitId);
        UNIT_ASSERT_VALUES_EQUAL(
            confirmedCommitId,
            args.ConfirmedDataRefCommitId);
    }

    Y_UNIT_TEST(ShouldWriteUnconfirmedDataInParallel)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetWriteBlobThreshold(1);
        storageConfig.SetAddingUnconfirmedDataEnabled(true);

        TTestEnv env({}, std::move(storageConfig));
        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        ui64 handle = CreateHandle(tablet, id);
        const TString expectedFirst = BuildExpectedData({{4_KB, 'a'}});
        const TString expected = BuildExpectedData({{4_KB, 'a'}, {4_KB, 'b'}});

        AssertStorageStats(tablet, 0, 0);
        GenerateBlobIdsPutBlobAndConfirm(env, tablet, id, handle, 0, 4_KB, 'a');
        AssertStorageStats(tablet, 0, 0);
        UNIT_ASSERT_VALUES_EQUAL(
            expectedFirst,
            ReadData(tablet, handle, expectedFirst.size(), 0));
        GenerateBlobIdsPutBlobAndConfirm(
            env,
            tablet,
            id,
            handle,
            4_KB,
            4_KB,
            'b');
        AssertStorageStats(tablet, 0, 0);
        UNIT_ASSERT_VALUES_EQUAL(
            expected,
            ReadData(tablet, handle, expected.size(), 0));
        handle = RebootTabletAndCreateHandle(tablet, id);
        AssertStorageStats(tablet, 0, 0);
        UNIT_ASSERT_VALUES_EQUAL(
            expected,
            ReadData(tablet, handle, expected.size(), 0));
    }

    Y_UNIT_TEST(ShouldConfirmUnalignedHeadAndTailData)
    {
        constexpr ui32 block = 4_KB;

        NProto::TStorageConfig storageConfig;
        storageConfig.SetWriteBlobThreshold(1);
        storageConfig.SetAddingUnconfirmedDataEnabled(true);
        storageConfig.SetUnconfirmedDataCountHardLimit(10);

        TTestEnv env({}, std::move(storageConfig));
        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        auto& runtime = env.GetRuntime();

        TIndexTabletClient tablet(runtime, nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        ui64 handle = CreateHandle(tablet, id);

        constexpr ui64 writeOffset = 100;
        constexpr ui64 alignedOffset = block;
        constexpr ui64 alignedLength = block;
        constexpr ui64 tailOffset = alignedOffset + alignedLength;
        constexpr ui64 headLength = alignedOffset - writeOffset;
        constexpr ui64 tailLength = 300;
        constexpr ui64 writeLength = headLength + alignedLength + tailLength;

        TString headData(headLength, 'h');
        TString alignedData(alignedLength, 'a');
        TString tailData(tailLength, 't');

        auto request = tablet.CreateGenerateBlobIdsRequest(
            id,
            handle,
            alignedOffset,
            alignedLength);
        auto* head = request->Record.AddUnalignedDataRanges();
        head->SetOffset(writeOffset);
        head->SetContent(headData);

        auto* tail = request->Record.AddUnalignedDataRanges();
        tail->SetOffset(tailOffset);
        tail->SetContent(tailData);

        tablet.SendRequest(std::move(request));
        auto gbi = tablet.RecvGenerateBlobIdsResponse();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, gbi->GetStatus());
        UNIT_ASSERT(gbi->Record.GetUnconfirmedFlowEnabled());
        UNIT_ASSERT_VALUES_EQUAL(1, gbi->Record.BlobsSize());

        GenerateBlobIdsPutBlobAndConfirm(env, tablet, *gbi, alignedData);

        TString expected = headData + alignedData + tailData;

        UNIT_ASSERT_VALUES_EQUAL(
            expected,
            ReadData(tablet, handle, writeLength, writeOffset));

        auto firstBlock = tablet.ReadData(handle, 0, block);
        UNIT_ASSERT_VALUES_EQUAL(
            TString(writeOffset, '\0'),
            TString(firstBlock->Record.GetBuffer().substr(0, writeOffset)));
        UNIT_ASSERT_VALUES_EQUAL(
            headData,
            TString(TStringBuf(firstBlock->Record.GetBuffer())
                        .SubStr(writeOffset, headLength)));

        AssertStorageStats(tablet, 0, 0);
    }

    Y_UNIT_TEST(ShouldLimitUnconfirmedDataCount)
    {
        const ui32 block = 4_KB;

        NProto::TStorageConfig storageConfig;
        storageConfig.SetWriteBlobThreshold(1);
        storageConfig.SetAddingUnconfirmedDataEnabled(true);
        storageConfig.SetUnconfirmedDataCountHardLimit(3);

        TTestEnv env({}, std::move(storageConfig));
        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        ui64 handle = CreateHandle(tablet, id);
        AssertStorageStats(tablet, 0, 0);

        auto writeViaThreeStage = [&](ui64 offset, char fillChar) -> ui64
        {
            auto commitId = GenerateBlobIdsAndPutBlob(
                env,
                tablet,
                id,
                handle,
                offset,
                block,
                fillChar);
            env.GetRuntime().DispatchEvents({}, TDuration::MilliSeconds(100));
            return commitId;
        };

        auto commitId1 = writeViaThreeStage(0, 'a');
        AssertStorageStats(tablet, 1, 0);
        auto commitId2 = writeViaThreeStage(block, 'b');
        AssertStorageStats(tablet, 2, 0);
        auto commitId3 = writeViaThreeStage(2 * block, 'c');
        AssertStorageStats(tablet, 3, 0);

        auto gbiFourth = tablet.GenerateBlobIds(id, handle, 3 * block, block);
        UNIT_ASSERT(!gbiFourth->Record.GetUnconfirmedFlowEnabled());
        const ui64 commitId4 =
            GenerateBlobIdsAndPutBlob(env, *gbiFourth, TString(block, 'd'));
        TVector<TLogoBlobID> blobIds = {LogoBlobIDFromLogoBlobID(
            gbiFourth->Record.GetBlobs(0).GetBlobId())};
        tablet.AddData(id, handle, 3 * block, block, blobIds, commitId4);
        AssertStorageStats(tablet, 3, 0);
        UNIT_ASSERT_BUFFER_CONTENTS_EQUAL(
            ReadData(tablet, handle, block, 3 * block),
            block,
            'd');

        tablet.ConfirmAddData(commitId1);
        tablet.ConfirmAddData(commitId2);
        tablet.ConfirmAddData(commitId3);
        AssertStorageStats(tablet, 0, 0);

        UNIT_ASSERT_BUFFER_CONTENTS_EQUAL(
            ReadData(tablet, handle, block, 0),
            block,
            'a');
        UNIT_ASSERT_BUFFER_CONTENTS_EQUAL(
            ReadData(tablet, handle, block, block),
            block,
            'b');
        UNIT_ASSERT_BUFFER_CONTENTS_EQUAL(
            ReadData(tablet, handle, block, 2 * block),
            block,
            'c');
        UNIT_ASSERT_BUFFER_CONTENTS_EQUAL(
            ReadData(tablet, handle, block, 3 * block),
            block,
            'd');
    }

    Y_UNIT_TEST(ShouldMakeFirstConfirmedWriteVisibleFirst)
    {
        const ui32 block = 4_KB;

        NProto::TStorageConfig storageConfig;
        storageConfig.SetWriteBlobThreshold(1);
        storageConfig.SetAddingUnconfirmedDataEnabled(true);
        storageConfig.SetUnconfirmedDataCountHardLimit(10);

        TTestEnv env({}, std::move(storageConfig));
        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        ui64 handle = CreateHandle(tablet, id);

        // Two overlapping writes to the same range.
        const ui64 olderCommitId =
            GenerateBlobIdsAndPutBlob(env, tablet, id, handle, 0, block, 'a');
        const TString expected_a = BuildExpectedData({{block, 'a'}});
        const ui64 newerCommitId =
            GenerateBlobIdsAndPutBlob(env, tablet, id, handle, 0, block, 'b');
        const TString expected_b = BuildExpectedData({{block, 'b'}});
        UNIT_ASSERT_GT(newerCommitId, olderCommitId);

        AssertStorageStats(tablet, 2, 0);

        tablet.ConfirmAddData(newerCommitId);
        UNIT_ASSERT_VALUES_EQUAL(
            expected_b,
            ReadData(tablet, handle, expected_b.size(), 0));

        tablet.ConfirmAddData(olderCommitId);
        UNIT_ASSERT_VALUES_EQUAL(
            expected_a,
            ReadData(tablet, handle, expected_a.size(), 0));

        AssertStorageStats(tablet, 0, 0);
    }

    Y_UNIT_TEST(ShouldKeepPendingConfirmUntilAddDataUnconfirmedCompletes)
    {
        constexpr ui32 block = 4_KB;

        NProto::TStorageConfig storageConfig;
        storageConfig.SetWriteBlobThreshold(1);
        storageConfig.SetAddingUnconfirmedDataEnabled(true);
        storageConfig.SetUnconfirmedDataCountHardLimit(10);

        TTestEnv env({}, std::move(storageConfig));
        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        ui64 handle = CreateHandle(tablet, id);

        auto& runtime = env.GetRuntime();
        TAutoPtr<IEventHandle> addDataUnconfirmedCommitResult;

        runtime.SetEventFilter(
            [&](auto& runtime, auto& event)
            {
                Y_UNUSED(runtime);
                if (event->GetTypeRewrite() == TEvTablet::EvCommitResult &&
                    !addDataUnconfirmedCommitResult)
                {
                    addDataUnconfirmedCommitResult = event.Release();
                    return true;
                }
                return false;
            });

        auto gbi = tablet.GenerateBlobIds(id, handle, 0, block);
        const ui64 commitId = gbi->Record.GetCommitId();

        tablet.SendConfirmAddDataRequest(commitId);

        runtime.DispatchEvents(
            TDispatchOptions{
                .CustomFinalCondition = [&]()
                { return !!addDataUnconfirmedCommitResult; }},
            TDuration::Seconds(1));

        // ConfirmAddData stays pending while AddDataUnconfirmed commit is held.
        tablet.AssertConfirmAddDataNoResponse();
        AssertStorageStats(tablet, 1, 0);

        runtime.AdvanceCurrentTime(TDuration::Seconds(15));
        runtime.DispatchEvents({}, TDuration::MilliSeconds(100));

        // Pending confirmation is not cleaned up by timeout.
        tablet.AssertConfirmAddDataNoResponse();

        runtime.SetEventFilter(TTestActorRuntimeBase::DefaultFilterFunc);
        runtime.Send(addDataUnconfirmedCommitResult.Release(), nodeIdx);
        runtime.DispatchEvents({}, TDuration::MilliSeconds(100));

        // AddDataUnconfirmed eventually completed, so deferred confirm
        // succeeds.
        tablet.AssertConfirmAddDataResponse(S_OK);

        AssertStorageStats(tablet, 0, 0);
    }

    Y_UNIT_TEST(ShouldRejectConfirmAddDataForUnknownCommit)
    {
        const auto profileLog = std::make_shared<TTestProfileLog>();

        NProto::TStorageConfig storageConfig;
        storageConfig.SetWriteBlobThreshold(1);
        storageConfig.SetAddingUnconfirmedDataEnabled(true);
        storageConfig.SetUnconfirmedDataCountHardLimit(10);

        TTestEnv env({}, std::move(storageConfig), {}, profileLog);
        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        constexpr ui64 unknownCommitId = 424242;
        tablet.SendConfirmAddDataRequest(unknownCommitId);
        auto confirmResponse = tablet.AssertConfirmAddDataResponse(E_REJECTED);
        UNIT_ASSERT_C(
            confirmResponse->GetErrorReason().find(
                "unconfirmed data not found") != TString::npos,
            confirmResponse->GetErrorReason());
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            profileLog
                ->Requests[static_cast<ui32>(EFileStoreRequest::ConfirmAddData)]
                .size());
    }

    Y_UNIT_TEST(ShouldCancelAddDataWhileAddDataUnconfirmedInProgress)
    {
        constexpr ui32 block = 4_KB;

        NProto::TStorageConfig storageConfig;
        storageConfig.SetWriteBlobThreshold(1);
        storageConfig.SetAddingUnconfirmedDataEnabled(true);
        storageConfig.SetUnconfirmedDataCountHardLimit(10);

        TTestEnv env({}, std::move(storageConfig));
        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        auto& runtime = env.GetRuntime();

        TIndexTabletClient tablet(runtime, nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        ui64 handle = CreateHandle(tablet, id);

        TVector<TAutoPtr<IEventHandle>> heldCommitResults;
        runtime.SetEventFilter(
            [&](auto& runtime, auto& event)
            {
                Y_UNUSED(runtime);
                if (event->GetTypeRewrite() == TEvTablet::EvCommitResult) {
                    heldCommitResults.push_back(event.Release());
                    return true;
                }
                return false;
            });

        auto gbi = tablet.GenerateBlobIds(id, handle, 0, block);
        const ui64 commitId = gbi->Record.GetCommitId();

        tablet.SendCancelAddDataRequest(commitId);
        tablet.AssertCancelAddDataResponse(S_OK);

        // The first cancellation may enqueue delete while AddDataUnconfirmed
        // is still in progress; repeated cancellation should still be accepted.
        tablet.SendCancelAddDataRequest(commitId);
        tablet.AssertCancelAddDataResponse(S_OK);

        UNIT_ASSERT_C(
            !heldCommitResults.empty(),
            "Expected commit results to be held by the test");

        std::sort(
            heldCommitResults.begin(),
            heldCommitResults.end(),
            [](const auto& lhs, const auto& rhs)
            {
                return lhs->template Get<TEvTablet::TEvCommitResult>()->Step <
                       rhs->template Get<TEvTablet::TEvCommitResult>()->Step;
            });

        runtime.SetEventFilter(TTestActorRuntimeBase::DefaultFilterFunc);
        for (auto& commitResult: heldCommitResults) {
            runtime.Send(commitResult.Release(), nodeIdx);
        }
        heldCommitResults.clear();
        runtime.DispatchEvents({}, TDuration::Seconds(1));

        AssertStorageStats(tablet, 0, 0);

        tablet.SendCancelAddDataRequest(commitId);
        auto finalCancelResponse =
            tablet.AssertCancelAddDataResponse(E_REJECTED);
        UNIT_ASSERT_C(
            finalCancelResponse->GetErrorReason().find(
                "unconfirmed data not found") != TString::npos,
            finalCancelResponse->GetErrorReason());
    }

    Y_UNIT_TEST(ShouldRejectCancelAddDataForUnknownCommit)
    {
        const auto profileLog = std::make_shared<TTestProfileLog>();

        NProto::TStorageConfig storageConfig;
        storageConfig.SetWriteBlobThreshold(1);
        storageConfig.SetAddingUnconfirmedDataEnabled(true);
        storageConfig.SetUnconfirmedDataCountHardLimit(10);

        TTestEnv env({}, std::move(storageConfig), {}, profileLog);
        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        constexpr ui64 unknownCommitId = 424242;
        tablet.SendCancelAddDataRequest(unknownCommitId);
        auto response = tablet.AssertCancelAddDataResponse(E_REJECTED);
        UNIT_ASSERT_C(
            response->GetErrorReason().find("unconfirmed data not found") !=
                TString::npos,
            response->GetErrorReason());
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            profileLog
                ->Requests[static_cast<ui32>(EFileStoreRequest::CancelAddData)]
                .size());
    }

    Y_UNIT_TEST(ShouldReplyToRepeatedCancelAddDataWhileDeleteInProgress)
    {
        constexpr ui32 block = 4_KB;

        NProto::TStorageConfig storageConfig;
        storageConfig.SetWriteBlobThreshold(1);
        storageConfig.SetAddingUnconfirmedDataEnabled(true);
        storageConfig.SetUnconfirmedDataCountHardLimit(10);

        TTestEnv env({}, std::move(storageConfig));
        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        auto& runtime = env.GetRuntime();

        TIndexTabletClient tablet(runtime, nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        ui64 handle = CreateHandle(tablet, id);

        const ui64 commitId =
            GenerateBlobIdsAndPutBlob(env, tablet, id, handle, 0, block, 'a');

        runtime.DispatchEvents({}, TDuration::MilliSeconds(100));

        AssertStorageStats(tablet, 1, 0);

        TAutoPtr<IEventHandle> deleteUnconfirmedCommitResult;
        runtime.SetEventFilter(
            [&](auto& runtime, auto& event)
            {
                Y_UNUSED(runtime);
                if (event->GetTypeRewrite() == TEvTablet::EvCommitResult &&
                    !deleteUnconfirmedCommitResult)
                {
                    deleteUnconfirmedCommitResult = event.Release();
                    return true;
                }
                return false;
            });

        tablet.SendCancelAddDataRequest(commitId);
        auto firstCancelResponse = tablet.RecvCancelAddDataResponse();
        UNIT_ASSERT(firstCancelResponse);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, firstCancelResponse->GetStatus());

        runtime.DispatchEvents(
            TDispatchOptions{
                .CustomFinalCondition = [&]()
                { return !!deleteUnconfirmedCommitResult; }},
            TDuration::Seconds(1));

        UNIT_ASSERT(deleteUnconfirmedCommitResult);

        tablet.SendCancelAddDataRequest(commitId);
        auto secondCancelResponse = tablet.RecvCancelAddDataResponse();
        UNIT_ASSERT(secondCancelResponse);
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, secondCancelResponse->GetStatus());

        runtime.SetEventFilter(TTestActorRuntimeBase::DefaultFilterFunc);
        runtime.Send(deleteUnconfirmedCommitResult.Release(), nodeIdx);
        runtime.DispatchEvents({}, TDuration::MilliSeconds(100));

        AssertStorageStats(tablet, 0, 0);
    }

}

}   // namespace NCloud::NFileStore::NStorage
