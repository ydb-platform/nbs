#include "validation.h"

#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/common/iovector.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/service_test.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/media.h>
#include <cloud/storage/core/libs/common/random.h>
#include <cloud/storage/core/libs/common/sglist_test.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore {

using namespace NThreading;
using namespace NUnitTest;

namespace {

////////////////////////////////////////////////////////////////////////////////

using TBlocksHolder = std::shared_ptr<TVector<TString>>;

TBlocksHolder
ResizeAndFillBlocksWithRandomData(TSgList& sglist, ui32 blockSize, ui32 blockCount)
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

void FillBlocksWithDeterministicData(
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
    request->BlocksCount = blocksCount;
    request->BlockSize = blockSize;

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
    ResizeAndFillBlocksWithRandomData(*request->MutableBlocks(), blockSize, blocksCount);
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
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TEncryptionClientTest)
{
    Y_UNIT_TEST(ShouldCalculateChecksumsForWriteRequests)
    {
        constexpr ui32 BlockSize = 4_KB;
        TTestEnv env{};
        auto dataIntegrityClient = NClient::CreateDataIntegrityClient(
            env.Logging,
            env.Monitoring,
            env.TestClient,
            NProto::STORAGE_MEDIA_SSD,
            BlockSize);

        constexpr ui32 maxBlockCount = MaxSubRequestSize / BlockSize;

        env.TestClient->WriteBlocksHandler =
            [&](std::shared_ptr<NProto::TWriteBlocksRequest> request)
        {
            const auto& checksums = request->GetChecksum().GetChecksums();
            UNIT_ASSERT_VALUES_EQUAL(2, checksums.size());
            UNIT_ASSERT_VALUES_EQUAL(821937825, checksums[0]);
            UNIT_ASSERT_VALUES_EQUAL(3818203452, checksums[1]);

            NProto::TWriteBlocksResponse response;
            return MakeFuture(std::move(response));
        };

        auto request = CreateWriteBlocksRequest(
            env.DiskId,
            BlockSize,
            42,   // startIndex
            maxBlockCount);

        ui8 value = 0;
        for (auto& buffer: *request->MutableBlocks()->MutableBuffers()) {
            for (char& byte: buffer) {
                byte = static_cast<char>(++value % 128);
            }
        }

        auto future = dataIntegrityClient->WriteBlocks(
            MakeIntrusive<TCallContext>(),
            request);
        auto response = future.GetValueSync();
        UNIT_ASSERT(!HasError(response.GetError()));
    }

    Y_UNIT_TEST(ShouldCalculateChecksumsForWriteLocalRequests)
    {
        constexpr ui32 BlockSize = 4_KB;
        TTestEnv env{};
        auto dataIntegrityClient = NClient::CreateDataIntegrityClient(
            env.Logging,
            env.Monitoring,
            env.TestClient,
            NProto::STORAGE_MEDIA_SSD,
            BlockSize);

        constexpr ui32 maxBlockCount = MaxSubRequestSize / BlockSize;

        env.TestClient->WriteBlocksLocalHandler =
            [&](std::shared_ptr<NProto::TWriteBlocksLocalRequest> request)
        {
            const auto& checksums = request->GetChecksum().GetChecksums();
            UNIT_ASSERT_VALUES_EQUAL(2, checksums.size());
            UNIT_ASSERT_VALUES_EQUAL(821937825, checksums[0]);
            UNIT_ASSERT_VALUES_EQUAL(3818203452, checksums[1]);

            NProto::TWriteBlocksLocalResponse response;
            return MakeFuture(std::move(response));
        };

        auto request = CreateWriteBlocksLocalRequest(
            env.BlocksHolderList,
            env.DiskId,
            BlockSize,
            42,   // startIndex
            maxBlockCount);

        ui8 value = 0;
        TVector<TString>& buffers = *env.BlocksHolderList[0];
        for (TString& buffer: buffers) {
            for (char& byte: buffer) {
                byte = static_cast<char>(++value % 128);
            }
        }
        auto future = dataIntegrityClient->WriteBlocksLocal(
            MakeIntrusive<TCallContext>(),
            request);
        auto response = future.GetValueSync();
        UNIT_ASSERT(!HasError(response.GetError()));
    }

    Y_UNIT_TEST(ShouldCalculateChecksumsForReadRequests)
    {
        constexpr ui32 BlockSize = 4_KB;
        TTestEnv env{};
        auto dataIntegrityClient = NClient::CreateDataIntegrityClient(
            env.Logging,
            env.Monitoring,
            env.TestClient,
            NProto::STORAGE_MEDIA_SSD,
            BlockSize);

        constexpr ui32 maxBlockCount = MaxSubRequestSize / BlockSize;

        env.TestClient->ReadBlocksHandler =
            [&](std::shared_ptr<NProto::TReadBlocksRequest> request)
        {
            NProto::TReadBlocksResponse response;
            FillBlocksWithDeterministicData(
                *response.MutableBlocks(),
                BlockSize,
                request->GetBlocksCount());
            response.MutableChecksum()->AddChecksums(675155616);
            return MakeFuture(std::move(response));
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
    }

    Y_UNIT_TEST(ShouldCalculateChecksumsForReadLocalRequests)
    {
        constexpr ui32 BlockSize = 4_KB;
        TTestEnv env{};
        auto dataIntegrityClient = NClient::CreateDataIntegrityClient(
            env.Logging,
            env.Monitoring,
            env.TestClient,
            NProto::STORAGE_MEDIA_SSD,
            BlockSize);

        constexpr ui32 maxBlockCount = MaxSubRequestSize / BlockSize;

        env.TestClient->ReadBlocksLocalHandler =
            [&](std::shared_ptr<NProto::TReadBlocksLocalRequest>)
        {
            NProto::TReadBlocksLocalResponse response;
            response.MutableChecksum()->AddChecksums(675155616);
            return MakeFuture(std::move(response));
        };

        auto request = CreateReadBlocksLocalRequest(
            env.BlocksHolderList,
            env.DiskId,
            BlockSize,
            42,   // startIndex
            maxBlockCount);
        FillBlocksWithDeterministicData(env.BlocksHolderList.back());

        auto future = dataIntegrityClient->ReadBlocksLocal(
            MakeIntrusive<TCallContext>(),
            request);
        auto response = future.GetValueSync();
        UNIT_ASSERT_C(
            !HasError(response.GetError()),
            TStringBuilder() << FormatError(response.GetError()));
    }

    void ShouldCalculateCorrectAmountOfChecksumsForWriteRequests(ui32 blockSize)
    {
        TTestEnv env{};
        auto dataIntegrityClient = NClient::CreateDataIntegrityClient(
            env.Logging,
            env.Monitoring,
            env.TestClient,
            NProto::STORAGE_MEDIA_SSD,
            blockSize);

        const ui32 maxBlockCount = MaxSubRequestSize / blockSize;

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

            const auto& checksums = request->GetChecksum().GetChecksums();
            UNIT_ASSERT_VALUES_EQUAL(checksumCount, checksums.size());

            NProto::TWriteBlocksResponse response;
            return MakeFuture(std::move(response));
        };

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
        }
    }

#define BLOCKSTORE_IMPLEMENT_TEST(mediaKind, blockSize, name, ...)       \
    Y_UNIT_TEST(                                                         \
        ShouldCalculateCorrectAmountOfChecksumsForWriteRequestsFor##name##_##blockSize) \
    {                                                                    \
        ShouldCalculateCorrectAmountOfChecksumsForWriteRequests(blockSize);             \
    }                                                                    \
    // BLOCKSTORE_IMPLEMENT_TEST

    BLOCKSTORE_IMPLEMENT_TEST(NCloud::NProto::STORAGE_MEDIA_SSD, 4_KB, SSD)
    BLOCKSTORE_IMPLEMENT_TEST(NCloud::NProto::STORAGE_MEDIA_SSD, 8_KB, SSD)
    BLOCKSTORE_IMPLEMENT_TEST(NCloud::NProto::STORAGE_MEDIA_SSD, 16_KB, SSD)
    // BLOCKSTORE_IMPLEMENT_TEST(NCloud::NProto::STORAGE_MEDIA_HYBRID, Hybrid)
    // BLOCKSTORE_IMPLEMENT_TEST(NCloud::NProto::STORAGE_MEDIA_HDD, HDD)
    // BLOCKSTORE_IMPLEMENT_TEST(NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
    // SSDNonreplicated)
    // BLOCKSTORE_IMPLEMENT_TEST(NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR2,
    // SSDMirror2)
    // BLOCKSTORE_IMPLEMENT_TEST(NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR3,
    // SSDMirror3)
    // BLOCKSTORE_IMPLEMENT_TEST(NCloud::NProto::STORAGE_MEDIA_SSD_LOCAL,
    // SSDLocal)

#undef BLOCKSTORE_IMPLEMENT_TEST
}

}   // namespace NCloud::NBlockStore
