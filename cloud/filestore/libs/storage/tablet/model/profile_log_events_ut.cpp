#include "profile_log_events.h"

#include <cloud/filestore/libs/diagnostics/events/profile_events.ev.pb.h>
#include <cloud/filestore/libs/diagnostics/profile_log.h>
#include <cloud/filestore/libs/storage/tablet/model/blob.h>
#include <cloud/filestore/libs/storage/tablet/model/block.h>

#include <cloud/storage/core/libs/tablet/model/partial_blob_id.h>
#include <cloud/storage/core/protos/error.pb.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTestProfileLog
    : IProfileLog
{
    IProfileLog::TRecord Record;

    void Start() override
    {}

    void Stop() override
    {}

    void Write(IProfileLog::TRecord record) override
    {
        Record = std::move(record);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TProfileLogEvent)
{
    Y_UNIT_TEST(ShouldGetCorrectRequestName)
    {
        UNIT_ASSERT_VALUES_EQUAL(
            "Flush",
            GetFileStoreSystemRequestName(EFileStoreSystemRequest::Flush));

        UNIT_ASSERT_VALUES_EQUAL(
            "FlushBytes",
            GetFileStoreSystemRequestName(EFileStoreSystemRequest::FlushBytes));

        UNIT_ASSERT_VALUES_EQUAL(
            "Compaction",
            GetFileStoreSystemRequestName(EFileStoreSystemRequest::Compaction));

        UNIT_ASSERT_VALUES_EQUAL(
            "Cleanup",
            GetFileStoreSystemRequestName(EFileStoreSystemRequest::Cleanup));

        UNIT_ASSERT_VALUES_EQUAL(
            "TrimBytes",
            GetFileStoreSystemRequestName(EFileStoreSystemRequest::TrimBytes));

        UNIT_ASSERT_VALUES_EQUAL(
            "CollectGarbage",
            GetFileStoreSystemRequestName(EFileStoreSystemRequest::CollectGarbage));

        UNIT_ASSERT_VALUES_EQUAL(
            "DeleteGarbage",
            GetFileStoreSystemRequestName(EFileStoreSystemRequest::DeleteGarbage));

        UNIT_ASSERT_VALUES_EQUAL(
            "ReadBlob",
            GetFileStoreSystemRequestName(EFileStoreSystemRequest::ReadBlob));

        UNIT_ASSERT_VALUES_EQUAL(
            "WriteBlob",
            GetFileStoreSystemRequestName(EFileStoreSystemRequest::WriteBlob));

        UNIT_ASSERT_VALUES_EQUAL(
            "AddBlob",
            GetFileStoreSystemRequestName(EFileStoreSystemRequest::AddBlob));

        UNIT_ASSERT_VALUES_EQUAL(
            "TruncateRange",
            GetFileStoreSystemRequestName(EFileStoreSystemRequest::TruncateRange));

        UNIT_ASSERT_VALUES_EQUAL(
            "ZeroRange",
            GetFileStoreSystemRequestName(EFileStoreSystemRequest::ZeroRange));
    }

    Y_UNIT_TEST(ShouldCorrectlyInitProfileLogRequest)
    {
        const auto timestamp = TInstant::MilliSeconds(12);

        NProto::TProfileLogRequestInfo profileLogRequest;
        InitProfileLogRequestInfo(profileLogRequest, timestamp);

        UNIT_ASSERT_VALUES_EQUAL(
            timestamp.MicroSeconds(),
            profileLogRequest.GetTimestampMcs());
        UNIT_ASSERT(!profileLogRequest.HasDurationMcs());
        UNIT_ASSERT(!profileLogRequest.HasRequestType());
        UNIT_ASSERT(!profileLogRequest.HasErrorCode());
        UNIT_ASSERT_VALUES_EQUAL(0, profileLogRequest.GetRanges().size());
        UNIT_ASSERT(!profileLogRequest.HasNodeInfo());
        UNIT_ASSERT(!profileLogRequest.HasLockInfo());
    }

    Y_UNIT_TEST(ShouldCorrectlyInitProfileLogRequestWithRequestType)
    {
        const auto timestamp = TInstant::MilliSeconds(12);
        const auto requestType = EFileStoreSystemRequest::Compaction;

        NProto::TProfileLogRequestInfo profileLogRequest;
        InitProfileLogRequestInfo(profileLogRequest, requestType, timestamp);

        UNIT_ASSERT_VALUES_EQUAL(
            timestamp.MicroSeconds(),
            profileLogRequest.GetTimestampMcs());
        UNIT_ASSERT(!profileLogRequest.HasDurationMcs());
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(requestType),
            profileLogRequest.GetRequestType());
        UNIT_ASSERT(!profileLogRequest.HasErrorCode());
        UNIT_ASSERT_VALUES_EQUAL(0, profileLogRequest.GetRanges().size());
        UNIT_ASSERT(!profileLogRequest.HasNodeInfo());
        UNIT_ASSERT(!profileLogRequest.HasLockInfo());
    }

    Y_UNIT_TEST(ShouldCorrectlyFinalizeProfileLogRequest)
    {
        const auto startTs = TInstant::MilliSeconds(2);
        const auto endTs = TInstant::MilliSeconds(12);
        const TString fileSystemId = "test_filesystem";

        NCloud::NProto::TError error;
        error.SetCode(1);

        auto profileLog = std::make_shared<TTestProfileLog>();

        NProto::TProfileLogRequestInfo profileLogRequest;
        profileLogRequest.SetTimestampMcs(startTs.MicroSeconds());
        FinalizeProfileLogRequestInfo(
            std::move(profileLogRequest),
            endTs,
            fileSystemId,
            error,
            profileLog);

        UNIT_ASSERT_VALUES_EQUAL(fileSystemId, profileLog->Record.FileSystemId);
        UNIT_ASSERT_VALUES_EQUAL(
            startTs.MicroSeconds(),
            profileLog->Record.Request.GetTimestampMcs());
        UNIT_ASSERT_VALUES_EQUAL(
            (endTs - startTs).MicroSeconds(),
            profileLog->Record.Request.GetDurationMcs());
        UNIT_ASSERT(!profileLogRequest.HasRequestType());
        UNIT_ASSERT_VALUES_EQUAL(
            error.GetCode(),
            profileLog->Record.Request.GetErrorCode());
        UNIT_ASSERT_VALUES_EQUAL(0, profileLog->Record.Request.GetRanges().size());
        UNIT_ASSERT(!profileLog->Record.Request.HasNodeInfo());
        UNIT_ASSERT(!profileLog->Record.Request.HasLockInfo());
    }

    Y_UNIT_TEST(ShouldAddRange)
    {
        {
            const ui64 nodeId = 42;
            const ui64 offset =  4096;
            const ui64 bytes = 128;

            NProto::TProfileLogRequestInfo profileLogRequest;
            AddRange(nodeId, offset, bytes, profileLogRequest);

            UNIT_ASSERT(!profileLogRequest.HasTimestampMcs());
            UNIT_ASSERT(!profileLogRequest.HasDurationMcs());
            UNIT_ASSERT(!profileLogRequest.HasRequestType());
            UNIT_ASSERT(!profileLogRequest.HasErrorCode());

            UNIT_ASSERT_VALUES_EQUAL(1, profileLogRequest.GetRanges().size());
            UNIT_ASSERT_VALUES_EQUAL(
                nodeId,
                profileLogRequest.GetRanges().at(0).GetNodeId());
            UNIT_ASSERT(!profileLogRequest.GetRanges().at(0).HasHandle());
            UNIT_ASSERT_VALUES_EQUAL(
                offset,
                profileLogRequest.GetRanges().at(0).GetOffset());
            UNIT_ASSERT_VALUES_EQUAL(
                bytes,
                profileLogRequest.GetRanges().at(0).GetBytes());

            UNIT_ASSERT(!profileLogRequest.HasNodeInfo());
            UNIT_ASSERT(!profileLogRequest.HasLockInfo());
        }

        {
            const ui64 nodeId = 123;
            const ui64 handle = 21;
            const ui64 offset =  512;
            const ui64 bytes = 32;

            NProto::TProfileLogRequestInfo profileLogRequest;
            AddRange(nodeId, handle, offset, bytes, profileLogRequest);

            UNIT_ASSERT(!profileLogRequest.HasTimestampMcs());
            UNIT_ASSERT(!profileLogRequest.HasDurationMcs());
            UNIT_ASSERT(!profileLogRequest.HasRequestType());
            UNIT_ASSERT(!profileLogRequest.HasErrorCode());

            UNIT_ASSERT_VALUES_EQUAL(1, profileLogRequest.GetRanges().size());
            UNIT_ASSERT_VALUES_EQUAL(
                nodeId,
                profileLogRequest.GetRanges().at(0).GetNodeId());
            UNIT_ASSERT_VALUES_EQUAL(
                handle,
                profileLogRequest.GetRanges().at(0).GetHandle());
            UNIT_ASSERT_VALUES_EQUAL(
                offset,
                profileLogRequest.GetRanges().at(0).GetOffset());
            UNIT_ASSERT_VALUES_EQUAL(
                bytes,
                profileLogRequest.GetRanges().at(0).GetBytes());

            UNIT_ASSERT(!profileLogRequest.HasNodeInfo());
            UNIT_ASSERT(!profileLogRequest.HasLockInfo());
        }
    }

    Y_UNIT_TEST(ShouldAddBlobsInfoForMixedBlob)
    {
        const ui32 blockSize = 4096;
        const auto oldBlobFirst = TMixedBlob(
            MakePartialBlobId(1, 1),
            {TBlock(1, 3, 0, 0), TBlock(1, 5, 0, 0), TBlock(3, 6, 0, 0)},
            TBlobCompressionInfo(),
            "content_1");
        const auto oldBlobSecond = TMixedBlob(
            MakePartialBlobId(1, 3),
            {TBlock(3, 10, 0, 0), TBlock(7, 5, 0, 0), TBlock(7, 6, 0, 0)},
            TBlobCompressionInfo(),
            "content_2");
        const auto emptyBlob = TMixedBlob(
            MakePartialBlobId(1, 3),
            {},
            TBlobCompressionInfo(),
            "");
        const auto newBlob = TMixedBlob(
            MakePartialBlobId(2, 1),
            {TBlock(1, 10, 0, 0)},
            TBlobCompressionInfo(),
            "content_3");

        NProto::TProfileLogRequestInfo profileLogRequest;
        AddBlobsInfo(
            blockSize,
            TVector<TMixedBlob>{oldBlobFirst, oldBlobSecond, emptyBlob, newBlob},
            profileLogRequest);

        UNIT_ASSERT(!profileLogRequest.HasTimestampMcs());
        UNIT_ASSERT(!profileLogRequest.HasDurationMcs());
        UNIT_ASSERT(!profileLogRequest.HasRequestType());
        UNIT_ASSERT(!profileLogRequest.HasErrorCode());
        UNIT_ASSERT_VALUES_EQUAL(4, profileLogRequest.GetBlobsInfo().size());

#define TEST_RANGE(index, rindex, nodeId, offset, bytes)                       \
    {                                                                          \
        const auto& info = profileLogRequest.GetBlobsInfo().at(index);         \
        const auto& range = info.GetRanges().at(rindex);                       \
        UNIT_ASSERT_VALUES_EQUAL(nodeId, range.GetNodeId());                   \
        UNIT_ASSERT(!range.HasHandle());                                       \
        UNIT_ASSERT_VALUES_EQUAL(offset, range.GetOffset());                   \
        UNIT_ASSERT_VALUES_EQUAL(bytes, range.GetBytes());                     \
    }                                                                          \
// TEST_RANGE

        TEST_RANGE(0, 0, 1, 3 * blockSize, blockSize);
        TEST_RANGE(0, 1, 1, 5 * blockSize, blockSize);
        TEST_RANGE(0, 2, 3, 6 * blockSize, blockSize);
        TEST_RANGE(1, 0, 3, 10 * blockSize, blockSize);
        TEST_RANGE(1, 1, 7, 5 * blockSize, 2 * blockSize);
        TEST_RANGE(3, 0, 1, 10 * blockSize, blockSize);

#undef TEST_RANGE

        UNIT_ASSERT(!profileLogRequest.HasNodeInfo());
        UNIT_ASSERT(!profileLogRequest.HasLockInfo());
    }

    Y_UNIT_TEST(ShouldAddBlobsInfoForMixedBlobMeta)
    {
        const ui32 blockSize = 512;
        const auto oldBlobFirst = TMixedBlobMeta(
            MakePartialBlobId(1, 1),
            {TBlock(1, 3, 0, 0), TBlock(1, 4, 0, 0), TBlock(1, 6, 0, 0)},
            TBlobCompressionInfo());
        const auto emptyBlob = TMixedBlobMeta(
            MakePartialBlobId(1, 3),
            {},
            TBlobCompressionInfo());
        const auto oldBlobSecond = TMixedBlobMeta(
            MakePartialBlobId(1, 3),
            {TBlock(2, 4, 0, 0), TBlock(2, 5, 0, 0), TBlock(3, 0, 0, 0)},
            TBlobCompressionInfo());
        const auto newBlob = TMixedBlobMeta(
            MakePartialBlobId(2, 1),
            {TBlock(1, 10, 0, 0)},
            TBlobCompressionInfo());

        NProto::TProfileLogRequestInfo profileLogRequest;
        AddBlobsInfo(
            blockSize,
            TVector<TMixedBlobMeta>{
                oldBlobFirst,
                emptyBlob,
                oldBlobSecond,
                newBlob},
            profileLogRequest);

        UNIT_ASSERT(!profileLogRequest.HasTimestampMcs());
        UNIT_ASSERT(!profileLogRequest.HasDurationMcs());
        UNIT_ASSERT(!profileLogRequest.HasRequestType());
        UNIT_ASSERT(!profileLogRequest.HasErrorCode());
        UNIT_ASSERT_VALUES_EQUAL(4, profileLogRequest.GetBlobsInfo().size());

#define TEST_RANGE(index, rindex, nodeId, offset, bytes)                       \
    {                                                                          \
        const auto& info = profileLogRequest.GetBlobsInfo().at(index);         \
        const auto& range = info.GetRanges().at(rindex);                       \
        UNIT_ASSERT_VALUES_EQUAL(nodeId, range.GetNodeId());                   \
        UNIT_ASSERT(!range.HasHandle());                                       \
        UNIT_ASSERT_VALUES_EQUAL(offset, range.GetOffset());                   \
        UNIT_ASSERT_VALUES_EQUAL(bytes, range.GetBytes());                     \
    }                                                                          \
// TEST_RANGE

        TEST_RANGE(0, 0, 1, 3 * blockSize, 2 * blockSize);
        TEST_RANGE(0, 1, 1, 6 * blockSize, blockSize);
        TEST_RANGE(2, 0, 2, 4 * blockSize, 2 * blockSize);
        TEST_RANGE(2, 1, 3, 0, blockSize);
        TEST_RANGE(3, 0, 1, 10 * blockSize, blockSize);

#undef TEST_RANGE

        UNIT_ASSERT(!profileLogRequest.HasNodeInfo());
        UNIT_ASSERT(!profileLogRequest.HasLockInfo());
    }

    Y_UNIT_TEST(ShouldAddBlobsInfoForMergedBlobMeta)
    {
        const ui32 blockSize = 1024;
        const auto oldBlobFirst = TMergedBlobMeta(
            MakePartialBlobId(1, 1),
            TBlock(1, 3, 0, 0),
            1);
        const auto emptyBlob = TMergedBlobMeta(
            MakePartialBlobId(1, 3),
            TBlock(2, 8, 0, 0),
            0);
        const auto oldBlobSecond = TMergedBlobMeta(
            MakePartialBlobId(1, 3),
            TBlock(2, 4, 0, 0),
            5);
        const auto newBlob = TMergedBlobMeta(
            MakePartialBlobId(2, 1),
            TBlock(1, 2, 0, 0),
            2);

        NProto::TProfileLogRequestInfo profileLogRequest;
        AddBlobsInfo(
            blockSize,
            TVector<TMergedBlobMeta>{
                oldBlobFirst,
                emptyBlob,
                oldBlobSecond,
                newBlob},
            profileLogRequest);

        UNIT_ASSERT(!profileLogRequest.HasTimestampMcs());
        UNIT_ASSERT(!profileLogRequest.HasDurationMcs());
        UNIT_ASSERT(!profileLogRequest.HasRequestType());
        UNIT_ASSERT(!profileLogRequest.HasErrorCode());
        UNIT_ASSERT_VALUES_EQUAL(4, profileLogRequest.GetBlobsInfo().size());

#define TEST_RANGE(index, nodeId, offset, bytes)                               \
    {                                                                          \
        const auto& info = profileLogRequest.GetBlobsInfo().at(index);         \
        const auto& range = info.GetRanges().at(0);                            \
        UNIT_ASSERT_VALUES_EQUAL(nodeId, range.GetNodeId());                   \
        UNIT_ASSERT(!range.HasHandle());                                       \
        UNIT_ASSERT_VALUES_EQUAL(offset, range.GetOffset());                   \
        UNIT_ASSERT_VALUES_EQUAL(bytes, range.GetBytes());                     \
    }                                                                          \
// TEST_RANGE

        TEST_RANGE(0, 1, 3 * blockSize, blockSize);
        TEST_RANGE(1, 2, 8 * blockSize, 0);
        TEST_RANGE(2, 2, 4 * blockSize, 5 * blockSize);
        TEST_RANGE(3, 1, 2 * blockSize, 2 * blockSize);

        UNIT_ASSERT(!profileLogRequest.HasNodeInfo());
        UNIT_ASSERT(!profileLogRequest.HasLockInfo());

        AddBlobsInfo(
            blockSize,
            TVector<TMergedBlobMeta>{emptyBlob},
            profileLogRequest);

        UNIT_ASSERT_VALUES_EQUAL(5, profileLogRequest.GetBlobsInfo().size());

        TEST_RANGE(0, 1, 3 * blockSize, blockSize);
        TEST_RANGE(1, 2, 8 * blockSize, 0);
        TEST_RANGE(2, 2, 4 * blockSize, 5 * blockSize);
        TEST_RANGE(3, 1, 2 * blockSize, 2 * blockSize);
        TEST_RANGE(4, 2, 8 * blockSize, 0);

#undef TEST_RANGE
    }

    Y_UNIT_TEST(ShouldAddCompactionRange)
    {
        NProto::TProfileLogRequestInfo profileLogRequest;
        AddCompactionRange(100500, 500100, 100, 500, 1000, profileLogRequest);

        UNIT_ASSERT(!profileLogRequest.HasTimestampMcs());
        UNIT_ASSERT(!profileLogRequest.HasDurationMcs());
        UNIT_ASSERT(!profileLogRequest.HasRequestType());
        UNIT_ASSERT(!profileLogRequest.HasErrorCode());

        UNIT_ASSERT_VALUES_EQUAL(1, profileLogRequest.GetCompactionRanges().size());
        UNIT_ASSERT_VALUES_EQUAL(
            100500,
            profileLogRequest.GetCompactionRanges().at(0).GetCommitId());
        UNIT_ASSERT_VALUES_EQUAL(
            500100,
            profileLogRequest.GetCompactionRanges().at(0).GetRangeId());
        UNIT_ASSERT_VALUES_EQUAL(
            100,
            profileLogRequest.GetCompactionRanges().at(0).GetBlobsCount());
        UNIT_ASSERT_VALUES_EQUAL(
            500,
            profileLogRequest.GetCompactionRanges().at(0).GetDeletionsCount());
        UNIT_ASSERT_VALUES_EQUAL(
            1000,
            profileLogRequest.GetCompactionRanges().at(0).GetGarbageBlocksCount());

        UNIT_ASSERT(!profileLogRequest.HasNodeInfo());
        UNIT_ASSERT(!profileLogRequest.HasLockInfo());
    }
}

}   // namespace NCloud::NFileStore::NStorage
