#include "tablet_schema.h"
#include "tablet_tx.h"

#include <cloud/filestore/libs/service/error.h>
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

////////////////////////////////////////////////////////////////////////////////

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

    void RegisterCounters(NMonitoring::TDynamicCounters& root) override
    {
        Y_UNUSED(root);
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

    // We emulate situation, where under high load, we received confirmation
    // for previously triggered AddDataUnconfirmed from GenerateBlobsIdRequest
    // before ExecuteTx even started. Later during ExecuteTx we face some error
    // condition and should correctly report it back as ConfirmAddData response.
    Y_UNIT_TEST(ShouldReplyToPendingConfirmOnAddDataUnconfirmedValidationError)
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
        TAutoPtr<IEventHandle> blockedRwPut;

        runtime.SetEventFilter(
            [&](auto& runtime, auto& event)
            {
                Y_UNUSED(runtime);
                switch (event->GetTypeRewrite()) {
                    case TEvBlobStorage::EvPut: {
                        if (!blockedRwPut) {
                            blockedRwPut = event.Release();
                            return true;
                        }
                        break;
                    }
                }

                return false;
            });

        tablet.SendSetNodeAttrRequest(TSetNodeAttrArgs(RootNodeId).SetUid(1));
        runtime.DispatchEvents(
            TDispatchOptions{
                .CustomFinalCondition = [&]() { return !!blockedRwPut; }},
            TDuration::Seconds(1));

        tablet.AssertSetNodeAttrNoResponse();

        tablet.SendRequest(tablet.CreateUpdateConfigRequest(
            TFileSystemConfig{.BlockCount = 1}));

        auto gbi = tablet.GenerateBlobIds(id, handle, 0, 2 * block);
        const ui64 commitId = gbi->Record.GetCommitId();

        tablet.SendConfirmAddDataRequest(commitId);

        tablet.AssertConfirmAddDataNoResponse();

        runtime.SetEventFilter(TTestActorRuntimeBase::DefaultFilterFunc);
        runtime.Send(blockedRwPut.Release(), nodeIdx);
        runtime.DispatchEvents({}, TDuration::Seconds(1));

        auto setNodeAttrResponse = tablet.RecvSetNodeAttrResponse();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, setNodeAttrResponse->GetStatus());

        auto updateConfigResponse =
            tablet
                .RecvResponse<NKikimr::TEvFileStore::TEvUpdateConfigResponse>();
        UNIT_ASSERT_C(
            updateConfigResponse->Record.GetStatus() == NKikimrFileStore::OK,
            updateConfigResponse->Record.ShortDebugString());

        tablet.AssertConfirmAddDataResponse(ErrorNoSpaceLeft().GetCode());

        AssertStorageStats(tablet, 0, 0);
    }

    // We emulate situation, where under high load, we received cancellation
    // for previously triggered AddDataUnconfirmed from GenerateBlobsIdRequest
    // before ExecuteTx even started. For such cancel request we reply
    // immediately. Later during ExecuteTx we face some error condition and
    // check if deletion in progress. This test checks that we clean
    // UnconfirmedData.
    Y_UNIT_TEST(
        ShouldReplyToPendingConfirmOnAddDataUnconfirmedValidationErrorAfterCancel)
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
        TAutoPtr<IEventHandle> blockedRwPut;

        runtime.SetEventFilter(
            [&](auto& runtime, auto& event)
            {
                Y_UNUSED(runtime);
                switch (event->GetTypeRewrite()) {
                    case TEvBlobStorage::EvPut: {
                        if (!blockedRwPut) {
                            blockedRwPut = event.Release();
                            return true;
                        }
                        break;
                    }
                }

                return false;
            });

        tablet.SendSetNodeAttrRequest(TSetNodeAttrArgs(RootNodeId).SetUid(1));
        runtime.DispatchEvents(
            TDispatchOptions{
                .CustomFinalCondition = [&]() { return !!blockedRwPut; }},
            TDuration::Seconds(1));

        tablet.AssertSetNodeAttrNoResponse();

        tablet.SendRequest(tablet.CreateUpdateConfigRequest(
            TFileSystemConfig{.BlockCount = 1}));

        auto gbi = tablet.GenerateBlobIds(id, handle, 0, 2 * block);
        const ui64 commitId = gbi->Record.GetCommitId();

        tablet.SendCancelAddDataRequest(commitId);
        tablet.AssertCancelAddDataResponse(S_OK);

        runtime.SetEventFilter(TTestActorRuntimeBase::DefaultFilterFunc);
        runtime.Send(blockedRwPut.Release(), nodeIdx);
        runtime.DispatchEvents({}, TDuration::Seconds(1));

        auto setNodeAttrResponse = tablet.RecvSetNodeAttrResponse();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, setNodeAttrResponse->GetStatus());

        auto updateConfigResponse =
            tablet
                .RecvResponse<NKikimr::TEvFileStore::TEvUpdateConfigResponse>();
        UNIT_ASSERT_C(
            updateConfigResponse->Record.GetStatus() == NKikimrFileStore::OK,
            updateConfigResponse->Record.ShortDebugString());

        AssertStorageStats(tablet, 0, 0);
    }

    // We emulate situation, where we face some error condition during
    // AddUnconfirmedData and check if we successfully clear the barrier if no
    // deletion in progress for that UnconfirmedData.
    Y_UNIT_TEST(ShouldReleaseCollectBarrierOnAddDataUnconfirmedValidationError)
    {
        constexpr ui32 block = 4_KB;

        NProto::TStorageConfig storageConfig;
        storageConfig.SetWriteBlobThreshold(1);
        storageConfig.SetAddingUnconfirmedDataEnabled(true);
        storageConfig.SetUnconfirmedDataCountHardLimit(10);

        TTestEnv env({}, std::move(storageConfig));
        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            TFileSystemConfig{.BlockCount = 1});
        tablet.InitSession("client", "session");

        UNIT_ASSERT_EQUAL(
            tablet.GetStorageStats()
                ->Record.GetStats()
                .GetLastCollectCommitId(),
            0);

        auto id =
            CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "overflow"));
        ui64 handle = CreateHandle(tablet, id);

        const ui64 commitId = tablet.GenerateBlobIds(id, handle, 0, 2 * block)
                                  ->Record.GetCommitId();
        WaitForTabletCommit(env);

        AssertStorageStats(tablet, 0, 0);

        auto moveBarrier = [&]
        {
            auto node =
                CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "gc"));
            ui64 handle = CreateHandle(tablet, node);
            tablet.WriteData(handle, 0, block, 'a');
            tablet.Flush();
            tablet.DestroyHandle(handle);
            tablet.UnlinkNode(RootNodeId, "gc", false);
            tablet.CollectGarbage();
        };

        moveBarrier();

        UNIT_ASSERT_GT(
            tablet.GetStorageStats()
                ->Record.GetStats()
                .GetLastCollectCommitId(),
            commitId);
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

    Y_UNIT_TEST(ShouldConfirmData)
    {
        const ui32 block = 4_KB;
        const TString expectedFirst = BuildExpectedData({{block, 'a'}});
        const TString expected =
            BuildExpectedData({{block, 'a'}, {block, 'b'}});

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
        AssertStorageStats(tablet, 0, 0);

        bool dropConfirmAddData = false;

        env.GetRuntime().SetEventFilter(
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev)
            {
                switch (ev->GetTypeRewrite()) {
                    case TEvIndexTablet::EvConfirmAddDataRequest: {
                        if (dropConfirmAddData) {
                            return true;
                        }
                        break;
                    }
                }
                return false;
            });

        auto commitId =
            GenerateBlobIdsAndPutBlob(env, tablet, id, handle, 0, block, 'a');

        AssertStorageStats(tablet, 1, 0);
        tablet.ConfirmAddData(commitId);
        AssertStorageStats(tablet, 0, 0);
        UNIT_ASSERT_VALUES_EQUAL(
            expectedFirst,
            ReadData(tablet, handle, expectedFirst.size(), 0));

        dropConfirmAddData = true;
        auto commitId2 = GenerateBlobIdsAndPutBlob(
            env,
            tablet,
            id,
            handle,
            block,
            block,
            'b');

        AssertStorageStats(tablet, 1, 0);
        tablet.SendConfirmAddDataRequest(commitId2);
        env.GetRuntime().DispatchEvents({}, TDuration::MilliSeconds(100));
        AssertStorageStats(tablet, 1, 0);

        dropConfirmAddData = false;
        env.GetRuntime().SetEventFilter(
            [](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>&)
            { return false; });

        handle = RebootTabletAndCreateHandle(tablet, id);
        AssertStorageStats(tablet, 0, 0);
        UNIT_ASSERT_VALUES_EQUAL(
            expected,
            ReadData(tablet, handle, expected.size(), 0));
    }

    Y_UNIT_TEST(ShouldHandleCommitIdOverflowInAddDataUnconfirmed)
    {
        const ui32 block = 4_KB;
        const ui32 maxTabletStep = 40;

        NProto::TStorageConfig storageConfig;
        storageConfig.SetWriteBlobThreshold(1);
        storageConfig.SetAddingUnconfirmedDataEnabled(true);
        storageConfig.SetUnconfirmedDataCountHardLimit(10);
        storageConfig.SetMaxTabletStep(maxTabletStep);

        TTestEnv env({}, std::move(storageConfig));
        ui32 nodeIdx = env.AddDynamicNode();

        TTabletRebootTracker rebootTracker;
        auto rebootFilter = rebootTracker.GetEventFilter();
        bool confirmBlobsCompletedObserved = false;
        env.GetRuntime().SetEventFilter(
            [&](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event)
            {
                rebootFilter(runtime, event);

                if (event->GetTypeRewrite() ==
                    TEvIndexTabletPrivate::EvConfirmBlobsCompleted)
                {
                    confirmBlobsCompletedObserved = true;
                }

                return false;
            });

        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        ui64 handle = CreateHandle(tablet, id);

        // Force the next GenerateBlobIds to consume the last valid step.
        ui32 currentStep = 0;
        do {
            auto response = tablet.GenerateCommitId();
            const auto [generation, step] = ParseCommitId(response->CommitId);
            Y_UNUSED(generation);
            currentStep = step;
        } while (currentStep + 1 < maxTabletStep);
        UNIT_ASSERT_VALUES_EQUAL(maxTabletStep - 1, currentStep);

        // GenerateBlobIds uses the last available commit step.
        const ui64 commitId =
            GenerateBlobIdsAndPutBlob(env, tablet, id, handle, 0, block, 'a');
        {
            const auto [generation, step] = ParseCommitId(commitId);
            Y_UNUSED(generation);
            UNIT_ASSERT_VALUES_EQUAL(maxTabletStep, step);
        }

        // ConfirmAddData triggers AddBlobWriteUnconfirmed, which now overflows
        // its own GenerateCommitId and reboots the tablet.
        tablet.ConfirmAddData(commitId);
        env.GetRuntime().DispatchEvents({}, TDuration::MilliSeconds(100));

        UNIT_ASSERT_C(
            rebootTracker.IsPipeDestroyed(),
            "Expected tablet reboot on AddBlobWriteUnconfirmed commit "
            "overflow");

        tablet.ReconnectPipe();
        tablet.WaitReady();
        tablet.RecoverSession();
        handle = CreateHandle(tablet, id);
        rebootTracker.ClearPipeDestroyed();

        // Startup recovery should run blob confirmation on reboot.
        env.GetRuntime().DispatchEvents(
            TDispatchOptions{
                .CustomFinalCondition = [&]()
                { return confirmBlobsCompletedObserved; }},
            TDuration::Seconds(5));

        UNIT_ASSERT_C(
            confirmBlobsCompletedObserved,
            "Expected ConfirmBlobsCompleted during startup recovery");

        AssertStorageStats(tablet, 0, 0);
        UNIT_ASSERT_VALUES_EQUAL(
            TString(block, 'a'),
            ReadData(tablet, handle, block, 0));
    }

    Y_UNIT_TEST(ShouldLoadUnconfirmedDataAfterRestart)
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
        const TString expected = BuildExpectedData({{4_KB, 'a'}});

        AssertStorageStats(tablet, 0, 0);
        GenerateBlobIdsAndPutBlob(env, tablet, id, handle, 0, 4_KB, 'a');
        WaitForTabletCommit(env);
        AssertStorageStats(tablet, 1, 0);
        handle = RebootTabletAndCreateHandle(tablet, id);
        AssertStorageStats(tablet, 0, 0);
        UNIT_ASSERT_VALUES_EQUAL(
            expected,
            ReadData(tablet, handle, expected.size(), 0));
    }

    Y_UNIT_TEST(ShouldDropAllConfirmedDataWhenUnrecoverableBlobExists)
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
        AssertStorageStats(tablet, 0, 0);

        GenerateBlobIdsPutBlobAndConfirm(
            env,
            tablet,
            id,
            handle,
            0,
            block,
            'a');
        AssertStorageStats(tablet, 0, 0);

        auto writeUnconfirmed = [&](ui64 offset, char fillChar)
        {
            GenerateBlobIdsAndPutBlob(
                env,
                tablet,
                id,
                handle,
                offset,
                block,
                fillChar);
        };

        writeUnconfirmed(0, 'b');
        AssertStorageStats(tablet, 1, 0);
        writeUnconfirmed(0, 'c');
        AssertStorageStats(tablet, 2, 0);

        bool recoveryConfirmStarted = false;
        bool confirmBlobsCompleted = false;
        ui32 nodataInjected = 0;

        env.GetRuntime().SetEventFilter(
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev)
            {
                switch (ev->GetTypeRewrite()) {
                    case TEvIndexTabletPrivate::
                        EvLoadCompactionMapChunkResponse: {
                        const auto* msg =
                            ev->Get<TEvIndexTabletPrivate::
                                        TEvLoadCompactionMapChunkResponse>();
                        if (msg->LastRangeId == 0) {
                            recoveryConfirmStarted = true;
                        }
                        return false;
                    }
                    case TEvBlobStorage::EvGetResult: {
                        if (recoveryConfirmStarted && nodataInjected < 2) {
                            auto* msg = ev->Get<TEvBlobStorage::TEvGetResult>();
                            auto* mutableMsg =
                                const_cast<TEvBlobStorage::TEvGetResult*>(msg);

                            if (mutableMsg->ResponseSz > 0) {
                                mutableMsg->Responses[0].Status =
                                    NKikimrProto::NODATA;
                                ++nodataInjected;
                            }
                        }
                        return false;
                    }
                    case TEvIndexTabletPrivate::EvConfirmBlobsCompleted: {
                        confirmBlobsCompleted = true;
                        return false;
                    }
                }
                return false;
            });

        handle = RebootTabletAndCreateHandle(tablet, id);
        env.GetRuntime().DispatchEvents(
            TDispatchOptions{
                .CustomFinalCondition = [&]
                { return confirmBlobsCompleted && nodataInjected >= 2; }},
            TDuration::Seconds(5));
        UNIT_ASSERT(confirmBlobsCompleted);
        UNIT_ASSERT_VALUES_EQUAL(2, nodataInjected);
        AssertStorageStats(tablet, 0, 0);
        env.GetRuntime().SetEventFilter(
            TTestActorRuntimeBase::DefaultFilterFunc);
        UNIT_ASSERT_BUFFER_CONTENTS_EQUAL(
            ReadData(tablet, handle, block, 0),
            block,
            'a');
    }

    Y_UNIT_TEST(ShouldNotAllowWritingIfConfirmationNotReady)
    {
        constexpr ui32 block = 4_KB;

        NProto::TStorageConfig storageConfig;
        storageConfig.SetWriteBlobThreshold(1);
        storageConfig.SetAddingUnconfirmedDataEnabled(true);

        TTestEnv env({}, std::move(storageConfig));
        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);
        auto& runtime = env.GetRuntime();

        ui32 blockedConfirmBlobsCompleted = 0;
        bool holdBlockedConfirmBlobsCompleted = true;
        TAutoPtr<IEventHandle> heldConfirmBlobsCompleted;
        runtime.SetEventFilter(
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev)
            {
                if (ev->GetTypeRewrite() !=
                        TEvIndexTabletPrivate::EvConfirmBlobsCompleted ||
                    blockedConfirmBlobsCompleted >= 3)
                {
                    return false;
                }

                ++blockedConfirmBlobsCompleted;
                if (holdBlockedConfirmBlobsCompleted) {
                    heldConfirmBlobsCompleted = ev.Release();
                    holdBlockedConfirmBlobsCompleted = false;
                }

                return true;
            });

        TIndexTabletClient tablet(runtime, nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        ui64 handle = CreateHandle(tablet, id);

        GenerateBlobIdsAndPutBlob(env, tablet, id, handle, 0, 4_KB, 'a');
        WaitForTabletCommit(env);
        GenerateBlobIdsAndPutBlob(env, tablet, id, handle, block, block, 'b');
        WaitForTabletCommit(env);
        AssertStorageStats(tablet, 2, 0);

        handle = RebootTabletAndCreateHandle(tablet, id);
        runtime.DispatchEvents(
            TDispatchOptions{
                .CustomFinalCondition = [&]()
                { return !!heldConfirmBlobsCompleted; }},
            TDuration::Seconds(1));
        UNIT_ASSERT(heldConfirmBlobsCompleted);
        AssertStorageStats(tablet, 2, 0);

        // ConfirmBlobsCompleted is blocked, so writes must be rejected until
        // confirmation recovery is ready.
        tablet.SendWriteDataRequest(handle, 0, block, 'c', id);
        auto writeResponse = tablet.AssertWriteDataResponse(E_REJECTED);
        UNIT_ASSERT_C(
            writeResponse->GetErrorReason().find(
                "write overlaps with unconfirmed recovery data") !=
                TString::npos,
            writeResponse->GetErrorReason());

        runtime.SetEventFilter(TTestActorRuntimeBase::DefaultFilterFunc);
    }

    Y_UNIT_TEST(ShouldNotAllowReadingIfConfirmationNotReady)
    {
        constexpr ui32 block = 4_KB;

        NProto::TStorageConfig storageConfig;
        storageConfig.SetWriteBlobThreshold(1);
        storageConfig.SetAddingUnconfirmedDataEnabled(true);

        TTestEnv env({}, std::move(storageConfig));
        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);
        auto& runtime = env.GetRuntime();

        ui32 blockedConfirmBlobsCompleted = 0;
        bool holdBlockedConfirmBlobsCompleted = true;
        TAutoPtr<IEventHandle> heldConfirmBlobsCompleted;
        runtime.SetEventFilter(
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev)
            {
                if (ev->GetTypeRewrite() !=
                        TEvIndexTabletPrivate::EvConfirmBlobsCompleted ||
                    blockedConfirmBlobsCompleted >= 3)
                {
                    return false;
                }

                ++blockedConfirmBlobsCompleted;
                if (holdBlockedConfirmBlobsCompleted) {
                    heldConfirmBlobsCompleted = ev.Release();
                    holdBlockedConfirmBlobsCompleted = false;
                }

                return true;
            });

        TIndexTabletClient tablet(runtime, nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        ui64 handle = CreateHandle(tablet, id);

        GenerateBlobIdsAndPutBlob(env, tablet, id, handle, 0, 4_KB, 'a');
        WaitForTabletCommit(env);
        GenerateBlobIdsAndPutBlob(env, tablet, id, handle, block, block, 'b');
        WaitForTabletCommit(env);
        AssertStorageStats(tablet, 2, 0);

        handle = RebootTabletAndCreateHandle(tablet, id);
        runtime.DispatchEvents(
            TDispatchOptions{
                .CustomFinalCondition = [&]()
                { return !!heldConfirmBlobsCompleted; }},
            TDuration::Seconds(1));
        UNIT_ASSERT(heldConfirmBlobsCompleted);
        AssertStorageStats(tablet, 2, 0);

        // ConfirmBlobsCompleted is blocked, so overlapping reads must be
        // rejected until confirmation recovery is ready.
        tablet.SendReadDataRequest(handle, 0, block);
        auto readResponse = tablet.AssertReadDataResponse(E_REJECTED);
        UNIT_ASSERT_C(
            readResponse->GetErrorReason().find(
                "read overlaps with unconfirmed recovery data") !=
                TString::npos,
            readResponse->GetErrorReason());
    }

    Y_UNIT_TEST(
        ShouldDeleteUnconfirmedDataOnSessionInterruptionDuringCreateSession)
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

        TIndexTabletClient client1(runtime, nodeIdx, tabletId);
        client1.InitSession("client", "session");

        auto id =
            CreateNode(client1, TCreateNodeArgs::File(RootNodeId, "test"));
        ui64 handle = CreateHandle(client1, id);

        auto gbi = client1.GenerateBlobIds(id, handle, 0, block);
        Y_UNUSED(gbi);
        runtime.DispatchEvents({}, TDuration::Seconds(1));

        AssertStorageStats(client1, 1, 0);

        // Different owner recovers the same session => SessionInterrupted path.
        TIndexTabletClient client2(runtime, nodeIdx, tabletId);
        client2.InitSession("client", "session");
        runtime.DispatchEvents({}, TDuration::Seconds(1));

        AssertStorageStats(client2, 0, 0);
    }

    Y_UNIT_TEST(ShouldDeleteUnconfirmedDataOnDestroySession)
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

        auto gbi = tablet.GenerateBlobIds(id, handle, 0, block);
        Y_UNUSED(gbi);
        runtime.DispatchEvents({}, TDuration::Seconds(1));

        AssertStorageStats(tablet, 1, 0);

        tablet.DestroySession();
        runtime.DispatchEvents({}, TDuration::Seconds(1));

        AssertStorageStats(tablet, 0, 0);
    }

    Y_UNIT_TEST(ShouldDeleteUnconfirmedDataOnPipeDisconnection)
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

        TIndexTabletClient sessionClient(runtime, nodeIdx, tabletId);
        sessionClient.InitSession("client", "session");

        auto id = CreateNode(
            sessionClient,
            TCreateNodeArgs::File(RootNodeId, "test"));
        ui64 handle = CreateHandle(sessionClient, id);

        auto gbi = sessionClient.GenerateBlobIds(id, handle, 0, block);
        Y_UNUSED(gbi);
        runtime.DispatchEvents({}, TDuration::Seconds(1));

        AssertStorageStats(sessionClient, 1, 0);

        sessionClient.DisconnectPipe();
        runtime.DispatchEvents({}, TDuration::Seconds(1));

        TIndexTabletClient observer(runtime, nodeIdx, tabletId);
        AssertStorageStats(observer, 0, 0);
    }
}

}   // namespace NCloud::NFileStore::NStorage
