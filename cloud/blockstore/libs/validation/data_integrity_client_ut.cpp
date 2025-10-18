#include "validation.h"

#include <cloud/blockstore/libs/common/constants.h>
#include <cloud/blockstore/libs/common/iovector.h>
#include <cloud/blockstore/libs/common/request_checksum_helpers.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service_test.h>
#include <cloud/blockstore/private/api/protos/volume.pb.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/media.h>
#include <cloud/storage/core/libs/common/random.h>
#include <cloud/storage/core/libs/common/sglist_test.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/protobuf/json/json2proto.h>
#include <library/cpp/testing/unittest/registar.h>

#include <google/protobuf/util/message_differencer.h>

namespace NCloud::NBlockStore {

using namespace NThreading;
using namespace NUnitTest;

using MessageDifferencer = google::protobuf::util::MessageDifferencer;

namespace {

////////////////////////////////////////////////////////////////////////////////

using TBlocksHolder = std::shared_ptr<TVector<TString>>;

TBlocksHolder ResizeAndFillBlocksWithRandomData(
    TSgList& sglist,
    ui32 blockSize,
    ui32 blockCount)
{
    auto holder = std::make_shared<TVector<TString>>();
    auto data = RandomString(blockSize, RandInt<ui32>());
    sglist = ResizeBlocks(*holder, blockCount, data);
    return holder;
}

void ResizeAndFillBlocksWithRandomData(
    NProto::TIOVector& blockList,
    ui32 blockSize,
    ui32 blockCount)
{
    for (ui32 i = 0; i < blockCount; ++i) {
        auto data = RandomString(blockSize, RandInt<ui32>());
        blockList.AddBuffers(std::move(data));
    }
}

void FillBlocksWithDeterministicData(TBlocksHolder& holder)
{
    ui8 value = 0;
    for (TString& buffer: *holder) {
        for (char& byte: buffer) {
            byte = static_cast<char>(++value % 128);
        }
    }
}

void FillBlocksWithDeterministicData(const TSgList& sglist)
{
    ui8 value = 0;
    for (const TBlockDataRef& buffer: sglist) {
        for (size_t i = 0; i < buffer.Size(); ++i) {
            const_cast<char*>(buffer.Data())[i] =
                static_cast<char>(++value % 128);
        }
    }
}

void FillBlocksWithDeterministicData(NProto::TIOVector& blockList)
{
    ui8 value = 0;
    for (TString& buffer: *blockList.MutableBuffers()) {
        for (char& byte: buffer) {
            byte = static_cast<char>(++value % 128);
        }
    }
}

void AppendBlocksWithDeterministicData(
    NProto::TIOVector& blockList,
    ui32 blockSize,
    ui32 blockCount)
{
    ui8 value = 0;
    for (ui32 i = 0; i < blockCount; ++i) {
        TString data;
        data.reserve(blockSize);
        for (ui32 j = 0; j < blockSize; ++j) {
            data.push_back(static_cast<char>(++value % 128));
        }
        blockList.AddBuffers(std::move(data));
    }
}

std::shared_ptr<NProto::TWriteBlocksLocalRequest> CreateWriteBlocksLocalRequest(
    TVector<TBlocksHolder>& blocksHolderList,
    const TString& diskId,
    ui32 blockSize,
    ui64 startIndex,
    ui32 blocksCount)
{
    auto request = std::make_shared<NProto::TWriteBlocksLocalRequest>();
    request->SetDiskId(diskId);
    request->SetStartIndex(startIndex);
    request->SetBlockSize(blockSize);
    request->BlockSize = blockSize;
    request->BlocksCount = blocksCount;

    TSgList sglist;
    auto blocksHolder =
        ResizeAndFillBlocksWithRandomData(sglist, blockSize, blocksCount);
    blocksHolderList.push_back(blocksHolder);

    request->Sglist = TGuardedSgList(std::move(sglist));
    return request;
}

std::shared_ptr<NProto::TReadBlocksLocalRequest> CreateReadBlocksLocalRequest(
    TVector<TBlocksHolder>& blocksHolderList,
    const TString& diskId,
    ui32 blockSize,
    ui64 startIndex,
    ui32 blocksCount)
{
    TSgList sglist;
    auto blocksHolder =
        ResizeAndFillBlocksWithRandomData(sglist, blockSize, blocksCount);
    blocksHolderList.push_back(blocksHolder);

    auto request = std::make_shared<NProto::TReadBlocksLocalRequest>();
    request->SetDiskId(diskId);
    request->SetStartIndex(startIndex);
    request->SetBlocksCount(blocksCount);
    request->SetBlockSize(blockSize);
    request->BlockSize = blockSize;
    request->Sglist = TGuardedSgList(std::move(sglist));
    return request;
}

std::shared_ptr<NProto::TWriteBlocksRequest> CreateWriteBlocksRequest(
    const TString& diskId,
    ui32 blockSize,
    ui64 startIndex,
    ui32 blocksCount)
{
    auto request = std::make_shared<NProto::TWriteBlocksRequest>();
    request->SetDiskId(diskId);
    request->SetStartIndex(startIndex);
    request->SetBlockSize(blockSize);
    ResizeAndFillBlocksWithRandomData(
        *request->MutableBlocks(),
        blockSize,
        blocksCount);
    return request;
}

std::shared_ptr<NProto::TReadBlocksRequest> CreateReadBlocksRequest(
    const TString& diskId,
    ui64 startIndex,
    ui32 blocksCount)
{
    auto request = std::make_shared<NProto::TReadBlocksRequest>();
    request->SetDiskId(diskId);
    request->SetStartIndex(startIndex);
    request->SetBlocksCount(blocksCount);
    return request;
}

////////////////////////////////////////////////////////////////////////////////

struct TTestEnv
{
    const TString DiskId = "disk-id";

    TVector<TBlocksHolder> BlocksHolderList;
    ILoggingServicePtr Logging;
    IMonitoringServicePtr Monitoring;
    std::shared_ptr<TTestService> TestClient;

    TTestEnv()
    {
        Logging = CreateLoggingService("console");
        Monitoring = CreateMonitoringServiceStub();
        TestClient = std::make_shared<TTestService>();
    }

    ~TTestEnv() = default;

    IBlockStorePtr CreateDataIntegrityClient(
        ui32 blockSize,
        bool copiedValidationMode) const
    {
        NProto::TVolume volume;
        volume.SetBlockSize(blockSize);
        volume.SetDiskId("disk-id");
        if (copiedValidationMode) {
            volume.MutableTags()->insert(
                {TString(DataIntegrityViolationDetectedTagName), ""});
        }

        return NClient::CreateDataIntegrityClient(
            Logging,
            Monitoring,
            TestClient,
            volume);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDataIntegrityClientTest)
{
    Y_UNIT_TEST(ShouldCalculateChecksumsForWriteRequests)
    {
        constexpr ui32 BlockSize = 4_KB;
        TTestEnv env{};
        auto dataIntegrityClient = env.CreateDataIntegrityClient(
            BlockSize,
            /*copiedValidationMode=*/false);

        auto dataIntegrityCounters =
            env.Monitoring->GetCounters()
                ->GetSubgroup("counters", "blockstore")
                ->GetSubgroup("component", "service")
                ->GetSubgroup("subcomponent", "data_integrity");

        UNIT_ASSERT_VALUES_EQUAL(
            0,
            dataIntegrityCounters->GetSubgroup("validation_mode", "copied")
                ->GetCounter("Endpoints")
                ->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            dataIntegrityCounters->GetSubgroup("validation_mode", "direct")
                ->GetCounter("Endpoints")
                ->Val());

        constexpr ui32 maxBlockCount = MaxSubRequestSize / BlockSize;

        env.TestClient->WriteBlocksHandler =
            [&](std::shared_ptr<NProto::TWriteBlocksRequest> request)
        {
            const auto& checksums = request->GetChecksums();
            UNIT_ASSERT_VALUES_EQUAL(2, checksums.size());
            UNIT_ASSERT_VALUES_EQUAL(821937825, checksums[0].GetChecksum());
            UNIT_ASSERT_VALUES_EQUAL(
                (static_cast<ui64>(maxBlockCount) - 42) * BlockSize,
                checksums[0].GetByteCount());
            UNIT_ASSERT_VALUES_EQUAL(3818203452, checksums[1].GetChecksum());
            UNIT_ASSERT_VALUES_EQUAL(
                42 * BlockSize,
                checksums[1].GetByteCount());

            NProto::TWriteBlocksResponse response;
            return MakeFuture(std::move(response));
        };

        auto request = CreateWriteBlocksRequest(
            env.DiskId,
            BlockSize,
            42,   // startIndex
            maxBlockCount);
        FillBlocksWithDeterministicData(*request->MutableBlocks());

        auto future = dataIntegrityClient->WriteBlocks(
            MakeIntrusive<TCallContext>(),
            request);
        auto response = future.GetValueSync();
        UNIT_ASSERT(!HasError(response.GetError()));
        auto writeBlocksCounters =
            dataIntegrityCounters->GetSubgroup("request", "WriteBlocks");
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            writeBlocksCounters->GetCounter("Count")->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            writeBlocksCounters->GetCounter("Mismatches")->Val());
    }

    Y_UNIT_TEST(ShouldCalculateChecksumsForWriteLocalRequests)
    {
        constexpr ui32 BlockSize = 4_KB;
        TTestEnv env{};
        auto dataIntegrityClient = env.CreateDataIntegrityClient(
            BlockSize,
            /*copiedValidationMode=*/false);

        auto dataIntegrityCounters =
            env.Monitoring->GetCounters()
                ->GetSubgroup("counters", "blockstore")
                ->GetSubgroup("component", "service")
                ->GetSubgroup("subcomponent", "data_integrity");

        constexpr ui32 maxBlockCount = MaxSubRequestSize / BlockSize;

        env.TestClient->WriteBlocksLocalHandler =
            [&](std::shared_ptr<NProto::TWriteBlocksLocalRequest> request)
        {
            const auto& checksums = request->GetChecksums();
            UNIT_ASSERT_VALUES_EQUAL(2, checksums.size());
            UNIT_ASSERT_VALUES_EQUAL(821937825, checksums[0].GetChecksum());
            UNIT_ASSERT_VALUES_EQUAL(
                (static_cast<ui64>(maxBlockCount) - 42) * BlockSize,
                checksums[0].GetByteCount());
            UNIT_ASSERT_VALUES_EQUAL(3818203452, checksums[1].GetChecksum());
            UNIT_ASSERT_VALUES_EQUAL(
                42 * BlockSize,
                checksums[1].GetByteCount());

            NProto::TWriteBlocksLocalResponse response;
            return MakeFuture(std::move(response));
        };

        auto request = CreateWriteBlocksLocalRequest(
            env.BlocksHolderList,
            env.DiskId,
            BlockSize,
            42,   // startIndex
            maxBlockCount);
        FillBlocksWithDeterministicData(env.BlocksHolderList.back());

        auto future = dataIntegrityClient->WriteBlocksLocal(
            MakeIntrusive<TCallContext>(),
            request);
        auto response = future.GetValueSync();
        UNIT_ASSERT(!HasError(response.GetError()));
        auto writeBlocksLocalCounters =
            dataIntegrityCounters->GetSubgroup("validation_mode", "direct")
                ->GetSubgroup("request", "WriteBlocksLocal");
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            writeBlocksLocalCounters->GetCounter("Count")->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            writeBlocksLocalCounters->GetCounter("Mismatches")->Val());
    }

    Y_UNIT_TEST(ShouldCalculateChecksumsForReadRequests)
    {
        constexpr ui32 BlockSize = 4_KB;
        TTestEnv env{};
        auto dataIntegrityClient = env.CreateDataIntegrityClient(
            BlockSize,
            /*copiedValidationMode=*/false);

        auto dataIntegrityCounters =
            env.Monitoring->GetCounters()
                ->GetSubgroup("counters", "blockstore")
                ->GetSubgroup("component", "service")
                ->GetSubgroup("subcomponent", "data_integrity");

        constexpr ui32 maxBlockCount = MaxSubRequestSize / BlockSize;

        env.TestClient->ReadBlocksHandler =
            [&](std::shared_ptr<NProto::TReadBlocksRequest> request)
        {
            NProto::TReadBlocksResponse response;
            AppendBlocksWithDeterministicData(
                *response.MutableBlocks(),
                BlockSize,
                request->GetBlocksCount());
            response.MutableChecksum()->SetChecksum(675155616);
            response.MutableChecksum()->SetByteCount(
                static_cast<ui64>(request->GetBlocksCount()) * BlockSize);
            return MakeFuture<NProto::TReadBlocksResponse>(std::move(response));
        };

        auto request = CreateReadBlocksRequest(
            env.DiskId,
            42,   // startIndex
            maxBlockCount);

        auto future = dataIntegrityClient->ReadBlocks(
            MakeIntrusive<TCallContext>(),
            request);
        auto response = future.GetValueSync();
        UNIT_ASSERT_C(
            !HasError(response.GetError()),
            TStringBuilder() << FormatError(response.GetError()));
        auto readBlocksCounters =
            dataIntegrityCounters->GetSubgroup("request", "ReadBlocks");
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            readBlocksCounters->GetCounter("Count")->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            readBlocksCounters->GetCounter("Mismatches")->Val());
    }

    Y_UNIT_TEST(ShouldReturnErrorOnReadChecksumMismatch)
    {
        constexpr ui32 BlockSize = 4_KB;
        TTestEnv env{};
        auto dataIntegrityClient = env.CreateDataIntegrityClient(
            BlockSize,
            /*copiedValidationMode=*/false);

        auto dataIntegrityCounters =
            env.Monitoring->GetCounters()
                ->GetSubgroup("counters", "blockstore")
                ->GetSubgroup("component", "service")
                ->GetSubgroup("subcomponent", "data_integrity");

        constexpr ui32 maxBlockCount = MaxSubRequestSize / BlockSize;

        env.TestClient->ReadBlocksHandler =
            [&](std::shared_ptr<NProto::TReadBlocksRequest> request)
        {
            NProto::TReadBlocksResponse response;
            AppendBlocksWithDeterministicData(
                *response.MutableBlocks(),
                BlockSize,
                request->GetBlocksCount());
            response.MutableChecksum()->SetChecksum(0xbeef);
            response.MutableChecksum()->SetByteCount(
                static_cast<ui64>(request->GetBlocksCount()) * BlockSize);
            return MakeFuture<NProto::TReadBlocksResponse>(std::move(response));
        };

        auto request = CreateReadBlocksRequest(
            env.DiskId,
            42,   // startIndex
            maxBlockCount);

        auto future = dataIntegrityClient->ReadBlocks(
            MakeIntrusive<TCallContext>(),
            request);
        auto response = future.GetValueSync();
        UNIT_ASSERT_C(
            HasError(response.GetError()),
            TStringBuilder() << FormatError(response.GetError()));
        UNIT_ASSERT_EQUAL(E_REJECTED, response.GetError().GetCode());
        UNIT_ASSERT(HasProtoFlag(
            response.GetError().GetFlags(),
            NProto::EF_CHECKSUM_MISMATCH));
        auto readBlocksCounters =
            dataIntegrityCounters->GetSubgroup("request", "ReadBlocks");
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            readBlocksCounters->GetCounter("Count")->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            readBlocksCounters->GetCounter("Mismatches")->Val());
    }

    Y_UNIT_TEST(ShouldCalculateChecksumsForReadLocalRequests)
    {
        constexpr ui32 BlockSize = 4_KB;
        TTestEnv env{};
        auto dataIntegrityClient = env.CreateDataIntegrityClient(
            BlockSize,
            /*copiedValidationMode=*/false);

        constexpr ui32 maxBlockCount = MaxSubRequestSize / BlockSize;

        auto dataIntegrityCounters =
            env.Monitoring->GetCounters()
                ->GetSubgroup("counters", "blockstore")
                ->GetSubgroup("component", "service")
                ->GetSubgroup("subcomponent", "data_integrity");

        env.TestClient->ReadBlocksLocalHandler =
            [&](std::shared_ptr<NProto::TReadBlocksLocalRequest> request)
        {
            FillBlocksWithDeterministicData(request->Sglist.Acquire().Get());
            NProto::TReadBlocksLocalResponse response;
            response.MutableChecksum()->SetChecksum(675155616);
            response.MutableChecksum()->SetByteCount(
                static_cast<ui64>(request->GetBlocksCount()) * BlockSize);
            return MakeFuture<NProto::TReadBlocksLocalResponse>(
                std::move(response));
        };

        auto request = CreateReadBlocksLocalRequest(
            env.BlocksHolderList,
            env.DiskId,
            BlockSize,
            42,   // startIndex
            maxBlockCount);

        auto future = dataIntegrityClient->ReadBlocksLocal(
            MakeIntrusive<TCallContext>(),
            request);
        auto response = future.GetValueSync();
        UNIT_ASSERT_C(
            !HasError(response.GetError()),
            TStringBuilder() << FormatError(response.GetError()));
        auto readBlocksLocalCounters =
            dataIntegrityCounters->GetSubgroup("validation_mode", "direct")
                ->GetSubgroup("request", "ReadBlocksLocal");
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            readBlocksLocalCounters->GetCounter("Count")->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            readBlocksLocalCounters->GetCounter("Mismatches")->Val());
    }

    void ShouldCalculateCorrectAmountOfChecksumsForWriteRequests(ui32 blockSize)
    {
        TTestEnv env{};
        auto dataIntegrityClient = env.CreateDataIntegrityClient(
            blockSize,
            /*copiedValidationMode=*/false);

        const ui32 maxBlockCount = MaxSubRequestSize / blockSize;

        auto dataIntegrityCounters =
            env.Monitoring->GetCounters()
                ->GetSubgroup("counters", "blockstore")
                ->GetSubgroup("component", "service")
                ->GetSubgroup("subcomponent", "data_integrity");

        env.TestClient->WriteBlocksHandler =
            [&](std::shared_ptr<NProto::TWriteBlocksRequest> request)
        {
            const ui32 blockCount =
                CalculateWriteRequestBlockCount(*request, blockSize);
            const ui32 checksumCount =
                AlignUp<ui64>(request->GetStartIndex() + 1, maxBlockCount) >=
                        request->GetStartIndex() + blockCount
                    ? 1
                    : 2;

            const auto& checksums = request->GetChecksums();
            UNIT_ASSERT_VALUES_EQUAL(checksumCount, checksums.size());

            NProto::TWriteBlocksResponse response;
            return MakeFuture(std::move(response));
        };

        auto writeBlocksCounters =
            dataIntegrityCounters->GetSubgroup("request", "WriteBlocks");

        {
            auto request = CreateWriteBlocksRequest(
                env.DiskId,
                blockSize,
                0,    // startIndex
                1);   // blocksCount
            auto future = dataIntegrityClient->WriteBlocks(
                MakeIntrusive<TCallContext>(),
                request);
            auto response = future.GetValueSync();
            UNIT_ASSERT(!HasError(response.GetError()));

            UNIT_ASSERT_VALUES_EQUAL(
                1,
                writeBlocksCounters->GetCounter("Count")->Val());
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                writeBlocksCounters->GetCounter("Mismatches")->Val());
        }

        {
            auto request = CreateWriteBlocksRequest(
                env.DiskId,
                blockSize,
                0,   // startIndex
                maxBlockCount);
            auto future = dataIntegrityClient->WriteBlocks(
                MakeIntrusive<TCallContext>(),
                request);
            auto response = future.GetValueSync();
            UNIT_ASSERT(!HasError(response.GetError()));

            UNIT_ASSERT_VALUES_EQUAL(
                2,
                writeBlocksCounters->GetCounter("Count")->Val());
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                writeBlocksCounters->GetCounter("Mismatches")->Val());
        }

        {
            auto request = CreateWriteBlocksRequest(
                env.DiskId,
                blockSize,
                1,   // startIndex
                maxBlockCount);
            auto future = dataIntegrityClient->WriteBlocks(
                MakeIntrusive<TCallContext>(),
                request);
            auto response = future.GetValueSync();
            UNIT_ASSERT(!HasError(response.GetError()));

            UNIT_ASSERT_VALUES_EQUAL(
                3,
                writeBlocksCounters->GetCounter("Count")->Val());
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                writeBlocksCounters->GetCounter("Mismatches")->Val());
        }
    }

#define IMPLEMENT_CHECKSUMS_AMOUNT_CHECKER_TEST(blockSize)                        \
    Y_UNIT_TEST(                                                                  \
        ShouldCalculateCorrectAmountOfChecksumsForWriteRequestsWithBs##blockSize) \
    {                                                                             \
        ShouldCalculateCorrectAmountOfChecksumsForWriteRequests(blockSize);       \
    }                                                                             \
    // IMPLEMENT_CHECKSUMS_AMOUNT_CHECKER_TEST

    IMPLEMENT_CHECKSUMS_AMOUNT_CHECKER_TEST(4_KB)
    IMPLEMENT_CHECKSUMS_AMOUNT_CHECKER_TEST(8_KB)
    IMPLEMENT_CHECKSUMS_AMOUNT_CHECKER_TEST(16_KB)

#undef IMPLEMENT_CHECKSUMS_AMOUNT_CHECKER_TEST

    Y_UNIT_TEST(ShouldEnterCopiedDataValidationModeOnStart)
    {
        TTestEnv env{};
        constexpr ui32 BlockSize = 4_KB;
        auto dataIntegrityClient = env.CreateDataIntegrityClient(
            BlockSize,
            /*copiedValidationMode=*/true);

        const ui32 maxBlockCount = MaxSubRequestSize / BlockSize;

        auto dataIntegrityCounters =
            env.Monitoring->GetCounters()
                ->GetSubgroup("counters", "blockstore")
                ->GetSubgroup("component", "service")
                ->GetSubgroup("subcomponent", "data_integrity");
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            dataIntegrityCounters->GetSubgroup("validation_mode", "copied")
                ->GetCounter("Endpoints")
                ->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            dataIntegrityCounters->GetSubgroup("validation_mode", "direct")
                ->GetCounter("Endpoints")
                ->Val());
        auto writeBlocksLocalCounters =
            dataIntegrityCounters->GetSubgroup("validation_mode", "copied")
                ->GetSubgroup("request", "WriteBlocksLocal");
        auto readBlocksLocalCounters =
            dataIntegrityCounters->GetSubgroup("validation_mode", "copied")
                ->GetSubgroup("request", "ReadBlocksLocal");
        auto readBlocksCounters =
            dataIntegrityCounters->GetSubgroup("request", "ReadBlocks");
        auto writeBlocksCounters =
            dataIntegrityCounters->GetSubgroup("request", "WriteBlocks");

        env.TestClient->WriteBlocksLocalHandler =
            [&](std::shared_ptr<NProto::TWriteBlocksLocalRequest> request)
        {
            const auto& checksums = request->GetChecksums();
            UNIT_ASSERT_VALUES_EQUAL(2, checksums.size());
            UNIT_ASSERT_VALUES_EQUAL(821937825, checksums[0].GetChecksum());
            UNIT_ASSERT_VALUES_EQUAL(
                (static_cast<ui64>(maxBlockCount) - 42) * BlockSize,
                checksums[0].GetByteCount());
            UNIT_ASSERT_VALUES_EQUAL(3818203452, checksums[1].GetChecksum());
            UNIT_ASSERT_VALUES_EQUAL(
                42 * BlockSize,
                checksums[1].GetByteCount());

            auto dataChecksum =
                CalculateChecksum(request->Sglist.Acquire().Get());
            auto expectedChecksum = CombineChecksums(checksums);
            UNIT_ASSERT_C(
                MessageDifferencer::Equals(dataChecksum, expectedChecksum),
                TStringBuilder()
                    << "dataChecksum: " << dataChecksum.ShortUtf8DebugString()
                    << ", expectedChecksum: "
                    << expectedChecksum.ShortUtf8DebugString());

            NProto::TWriteBlocksLocalResponse response;
            return MakeFuture(std::move(response));
        };

        env.TestClient->ReadBlocksLocalHandler =
            [&](std::shared_ptr<NProto::TReadBlocksLocalRequest> request)
        {
            FillBlocksWithDeterministicData(request->Sglist.Acquire().Get());
            NProto::TReadBlocksLocalResponse response;
            response.MutableChecksum()->SetChecksum(675155616);
            response.MutableChecksum()->SetByteCount(
                request->GetBlocksCount() * BlockSize);
            return MakeFuture<NProto::TReadBlocksLocalResponse>(
                std::move(response));
        };

        {
            auto request = CreateWriteBlocksLocalRequest(
                env.BlocksHolderList,
                env.DiskId,
                BlockSize,
                42,   // startIndex
                maxBlockCount);
            FillBlocksWithDeterministicData(env.BlocksHolderList.back());
            auto future = dataIntegrityClient->WriteBlocksLocal(
                MakeIntrusive<TCallContext>(),
                request);
            auto response = future.GetValueSync();
            UNIT_ASSERT(!HasError(response.GetError()));
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                writeBlocksLocalCounters->GetCounter("Count")->Val());
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                writeBlocksLocalCounters->GetCounter("Mismatches")->Val());
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                writeBlocksCounters->GetCounter("Count")->Val());
        }

        {
            auto request = CreateReadBlocksLocalRequest(
                env.BlocksHolderList,
                env.DiskId,
                BlockSize,
                42,   // startIndex
                maxBlockCount);
            auto future = dataIntegrityClient->ReadBlocksLocal(
                MakeIntrusive<TCallContext>(),
                request);
            auto response = future.GetValueSync();
            UNIT_ASSERT(!HasError(response.GetError()));
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                readBlocksLocalCounters->GetCounter("Count")->Val());
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                readBlocksLocalCounters->GetCounter("Mismatches")->Val());
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                readBlocksCounters->GetCounter("Count")->Val());

            auto dataChecksum =
                CalculateChecksum(request->Sglist.Acquire().Get());
            UNIT_ASSERT_C(
                MessageDifferencer::Equals(
                    dataChecksum,
                    response.GetChecksum()),
                TStringBuilder()
                    << "dataChecksum: " << dataChecksum.ShortUtf8DebugString()
                    << ", expectedChecksum: "
                    << response.GetChecksum().ShortUtf8DebugString());
        }
    }

    Y_UNIT_TEST(ShouldTransitionToCopiedValidationModeOnChecksumMismatch)
    {
        TTestEnv env{};
        constexpr ui32 BlockSize = 4_KB;
        auto dataIntegrityClient = env.CreateDataIntegrityClient(
            BlockSize,
            /*copiedValidationMode=*/false);

        const ui32 maxBlockCount = MaxSubRequestSize / BlockSize;

        auto dataIntegrityCounters =
            env.Monitoring->GetCounters()
                ->GetSubgroup("counters", "blockstore")
                ->GetSubgroup("component", "service")
                ->GetSubgroup("subcomponent", "data_integrity");
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            dataIntegrityCounters->GetSubgroup("validation_mode", "copied")
                ->GetCounter("Endpoints")
                ->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            dataIntegrityCounters->GetSubgroup("validation_mode", "direct")
                ->GetCounter("Endpoints")
                ->Val());
        auto writeBlocksLocalCounters =
            dataIntegrityCounters->GetSubgroup("validation_mode", "direct")
                ->GetSubgroup("request", "WriteBlocksLocal");
        auto readBlocksLocalCounters =
            dataIntegrityCounters->GetSubgroup("validation_mode", "direct")
                ->GetSubgroup("request", "ReadBlocksLocal");
        auto readBlocksCounters =
            dataIntegrityCounters->GetSubgroup("request", "ReadBlocks");
        auto writeBlocksCounters =
            dataIntegrityCounters->GetSubgroup("request", "WriteBlocks");

        // Handle read local request with checksum mismatch.
        env.TestClient->ReadBlocksLocalHandler =
            [&](std::shared_ptr<NProto::TReadBlocksLocalRequest> request)
        {
            FillBlocksWithDeterministicData(request->Sglist.Acquire().Get());
            NProto::TReadBlocksLocalResponse response;
            response.MutableChecksum()->SetChecksum(0xbeef);
            response.MutableChecksum()->SetByteCount(
                static_cast<ui64>(request->GetBlocksCount()) * BlockSize);
            return MakeFuture<NProto::TReadBlocksLocalResponse>(
                std::move(response));
        };

        env.TestClient->ExecuteActionHandler =
            [&](std::shared_ptr<NProto::TExecuteActionRequest> request)
        {
            UNIT_ASSERT_VALUES_EQUAL("modifytags", request->GetAction());
            NPrivateProto::TModifyTagsRequest input;
            NProtobufJson::Json2Proto(request->GetInput(), input);
            UNIT_ASSERT_VALUES_EQUAL(env.DiskId, input.GetDiskId());
            UNIT_ASSERT_VALUES_EQUAL(1, input.TagsToAddSize());
            UNIT_ASSERT_VALUES_EQUAL(
                DataIntegrityViolationDetectedTagName,
                input.GetTagsToAdd(0));
            return MakeFuture<NProto::TExecuteActionResponse>();
        };

        // Try to read local. After checksum mismatch the client should
        // transition to the copied validation mode.
        {
            auto request = CreateReadBlocksLocalRequest(
                env.BlocksHolderList,
                env.DiskId,
                BlockSize,
                42,   // startIndex
                maxBlockCount);
            auto future = dataIntegrityClient->ReadBlocksLocal(
                MakeIntrusive<TCallContext>(),
                request);
            auto response = future.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response.GetError().GetCode());
            UNIT_ASSERT(HasProtoFlag(
                response.GetError().GetFlags(),
                NProto::EF_INSTANT_RETRIABLE));
            UNIT_ASSERT(HasProtoFlag(
                response.GetError().GetFlags(),
                NProto::EF_CHECKSUM_MISMATCH));

            UNIT_ASSERT_VALUES_EQUAL(
                1,
                readBlocksLocalCounters->GetCounter("Count")->Val());
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                readBlocksLocalCounters->GetCounter("Mismatches")->Val());
        }

        // Check that the client has switched to the copied validation mode.
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            dataIntegrityCounters->GetSubgroup("validation_mode", "copied")
                ->GetCounter("Endpoints")
                ->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            dataIntegrityCounters->GetSubgroup("validation_mode", "direct")
                ->GetCounter("Endpoints")
                ->Val());

        env.TestClient->ExecuteActionHandler =
            [&](std::shared_ptr<NProto::TExecuteActionRequest>)
        {
            UNIT_ASSERT(false);   // should be unreachable
            return MakeFuture<NProto::TExecuteActionResponse>();
        };

        writeBlocksLocalCounters =
            dataIntegrityCounters->GetSubgroup("validation_mode", "copied")
                ->GetSubgroup("request", "WriteBlocksLocal");
        readBlocksLocalCounters =
            dataIntegrityCounters->GetSubgroup("validation_mode", "copied")
                ->GetSubgroup("request", "ReadBlocksLocal");

        env.TestClient->WriteBlocksLocalHandler =
            [&](std::shared_ptr<NProto::TWriteBlocksLocalRequest> request)
        {
            const auto& checksums = request->GetChecksums();
            UNIT_ASSERT_VALUES_EQUAL(2, checksums.size());
            UNIT_ASSERT_VALUES_EQUAL(821937825, checksums[0].GetChecksum());
            UNIT_ASSERT_VALUES_EQUAL(
                (static_cast<ui64>(maxBlockCount) - 42) * BlockSize,
                checksums[0].GetByteCount());
            UNIT_ASSERT_VALUES_EQUAL(3818203452, checksums[1].GetChecksum());
            UNIT_ASSERT_VALUES_EQUAL(
                42 * BlockSize,
                checksums[1].GetByteCount());

            auto dataChecksum =
                CalculateChecksum(request->Sglist.Acquire().Get());
            auto expectedChecksum = CombineChecksums(checksums);
            UNIT_ASSERT_C(
                MessageDifferencer::Equals(dataChecksum, expectedChecksum),
                TStringBuilder()
                    << "dataChecksum: " << dataChecksum.ShortUtf8DebugString()
                    << ", expectedChecksum: "
                    << expectedChecksum.ShortUtf8DebugString());

            NProto::TWriteBlocksLocalResponse response;
            return MakeFuture(std::move(response));
        };

        env.TestClient->ReadBlocksLocalHandler =
            [&](std::shared_ptr<NProto::TReadBlocksLocalRequest> request)
        {
            FillBlocksWithDeterministicData(request->Sglist.Acquire().Get());
            NProto::TReadBlocksLocalResponse response;
            response.MutableChecksum()->SetChecksum(675155616);
            response.MutableChecksum()->SetByteCount(
                request->GetBlocksCount() * BlockSize);
            return MakeFuture<NProto::TReadBlocksLocalResponse>(
                std::move(response));
        };

        {
            auto request = CreateReadBlocksLocalRequest(
                env.BlocksHolderList,
                env.DiskId,
                BlockSize,
                42,   // startIndex
                maxBlockCount);
            auto future = dataIntegrityClient->ReadBlocksLocal(
                MakeIntrusive<TCallContext>(),
                request);
            auto response = future.GetValueSync();
            UNIT_ASSERT(!HasError(response.GetError()));

            UNIT_ASSERT_VALUES_EQUAL(
                1,
                readBlocksLocalCounters->GetCounter("Count")->Val());
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                readBlocksLocalCounters->GetCounter("Mismatches")->Val());
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                readBlocksCounters->GetCounter("Count")->Val());

            // Counter without data coying should be incremented.
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                dataIntegrityCounters->GetSubgroup("validation_mode", "direct")
                    ->GetSubgroup("request", "ReadBlocksLocal")
                    ->GetCounter("Count")
                    ->Val());
        }

        {
            auto request = CreateWriteBlocksLocalRequest(
                env.BlocksHolderList,
                env.DiskId,
                BlockSize,
                42,   // startIndex
                maxBlockCount);
            FillBlocksWithDeterministicData(env.BlocksHolderList.back());
            auto future = dataIntegrityClient->WriteBlocksLocal(
                MakeIntrusive<TCallContext>(),
                request);
            auto response = future.GetValueSync();
            UNIT_ASSERT(!HasError(response.GetError()));
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                writeBlocksLocalCounters->GetCounter("Count")->Val());
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                writeBlocksLocalCounters->GetCounter("Mismatches")->Val());
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                writeBlocksCounters->GetCounter("Count")->Val());
        }

        env.TestClient->WriteBlocksHandler =
            [&](std::shared_ptr<NProto::TWriteBlocksRequest> request)
        {
            const auto& checksums = request->GetChecksums();
            UNIT_ASSERT_VALUES_EQUAL(2, checksums.size());
            UNIT_ASSERT_VALUES_EQUAL(821937825, checksums[0].GetChecksum());
            UNIT_ASSERT_VALUES_EQUAL(
                (static_cast<ui64>(maxBlockCount) - 42) * BlockSize,
                checksums[0].GetByteCount());
            UNIT_ASSERT_VALUES_EQUAL(3818203452, checksums[1].GetChecksum());
            UNIT_ASSERT_VALUES_EQUAL(
                42 * BlockSize,
                checksums[1].GetByteCount());

            auto dataChecksum = CalculateChecksum(
                request->GetBlocks(),
                request->GetBlockSize());
            auto expectedChecksum = CombineChecksums(checksums);
            UNIT_ASSERT_C(
                MessageDifferencer::Equals(dataChecksum, expectedChecksum),
                TStringBuilder()
                    << "dataChecksum: " << dataChecksum.ShortUtf8DebugString()
                    << ", expectedChecksum: "
                    << expectedChecksum.ShortUtf8DebugString());

            NProto::TWriteBlocksResponse response;
            return MakeFuture(std::move(response));
        };

        // Non-local write request should be done as usual.
        {
            auto request = CreateWriteBlocksRequest(
                env.DiskId,
                BlockSize,
                42,   // startIndex
                maxBlockCount);
            FillBlocksWithDeterministicData(*request->MutableBlocks());

            auto future = dataIntegrityClient->WriteBlocks(
                MakeIntrusive<TCallContext>(),
                request);
            auto response = future.GetValueSync();
            UNIT_ASSERT(!HasError(response.GetError()));
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                writeBlocksCounters->GetCounter("Count")->Val());
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                writeBlocksCounters->GetCounter("Mismatches")->Val());
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                writeBlocksLocalCounters->GetCounter("Count")->Val());
        }
    }
}

}   // namespace NCloud::NBlockStore
