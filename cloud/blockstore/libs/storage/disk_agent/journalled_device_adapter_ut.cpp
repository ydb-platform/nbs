#include "journalled_device_adapter.h"

#include <cloud/blockstore/libs/rdma_test/memory_test_storage.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/storage.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/device_client.h>

#include <cloud/fastshard/journal/iface/device.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/timer_test.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>
#include <util/random/random.h>

#include <chrono>
#include <functional>
#include <ranges>

namespace NCloud::NBlockStore::NStorage {

using namespace std::chrono_literals;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 DefaultBlockSize = 4_KB;
constexpr ui64 DefaultBlockCount = 1_MB / DefaultBlockSize;

TBuffer MakeBlock(size_t size, char c)
{
    TBuffer block;
    block.Fill(c, size);
    return block;
}

////////////////////////////////////////////////////////////////////////////////

struct TFixture: public NUnitTest::TBaseFixture
{
    const TString DeviceUUID = "uuid-1";
    const TInstant Now = TInstant::Seconds(1);

    ILoggingServicePtr Logging = CreateLoggingService("console");
    std::shared_ptr<TTestTimer> Timer = std::make_shared<TTestTimer>();

    std::shared_ptr<TMemoryTestStorage> Storage;
    TStorageAdapterPtr StorageAdapter;
    TDeviceClientPtr DeviceClient;
    NJournalled::IDevicePtr Device;

    void SetUp(NUnitTest::TTestContext& /*context*/) override
    {
        Storage = std::make_shared<TMemoryTestStorage>(
            DefaultBlockCount * DefaultBlockSize);

        StorageAdapter = std::make_shared<TStorageAdapter>(
            Storage,
            DefaultBlockSize,
            false,                    // normalize
            TDuration::Seconds(1),    // maxRequestDuration
            TDuration::Seconds(1));   // shutdownTimeout

        DeviceClient = std::make_shared<TDeviceClient>(
            10s,   // releaseInactiveSessionsTimeout
            TVector<std::pair<TString, TStorageAdapterPtr>>{
                {DeviceUUID, StorageAdapter}},
            Logging->CreateLog("BLOCKSTORE_DISK_AGENT"),
            false   // kickOutOldClientsEnabled
        );

        // the adapter does not check the client session, it is up to the
        // caller, so the device is not acquired here
        Timer->AdvanceTime(Now - TInstant::Zero());

        Device = CreateDeviceAdapter(
            Timer,
            DeviceUUID,
            DefaultBlockSize,
            DeviceClient);
    }

    static char BlockData(ui64 blockIndex)
    {
        return static_cast<char>('A' + blockIndex % 26);
    }

    void FillDevice()
    {
        auto request = std::make_shared<NProto::TWriteBlocksRequest>();
        request->SetStartIndex(0);
        request->SetBlockSize(DefaultBlockSize);

        auto& buffers = *request->MutableBlocks()->MutableBuffers();
        for (ui64 i = 0; i != DefaultBlockCount; ++i) {
            buffers.Add()->resize(DefaultBlockSize, BlockData(i));
        }

        const auto response = StorageAdapter->WriteBlocks(
            Now,
            CreateCallContext(),
            std::move(request),
            DefaultBlockSize,
            TStringBuf()   // dataBuffer
        ).GetValueSync();

        UNIT_ASSERT_C(!HasError(response), FormatError(response.GetError()));
    }

    NProto::TError WritePages(TVector<NJournalled::TPageRange> ranges)
    {
        return Device->WritePages(std::move(ranges)).GetValueSync();
    }

    auto ReadPages(TVector<NJournalled::TPageRangeRef> rangeRefs)
    {
        return Device->ReadPages(std::move(rangeRefs)).GetValueSync();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDeviceAdapterTest)
{
    Y_UNIT_TEST_F(ShouldValidateWritePagesRequest, TFixture)
    {
        using TPrepareFunc =
            std::function<void(TVector<NJournalled::TPageRange>&)>;

        const std::tuple<TPrepareFunc, NProto::TError> testCases[]{
            {[&](auto&) {}, MakeError(E_ARGUMENT, "nothing to write")},
            {[&](auto& ranges) { ranges.emplace_back(); },
             MakeError(E_ARGUMENT, "empty page group")},
            {[&](auto& ranges)
             {
                 auto& range = ranges.emplace_back();
                 range.FirstPageNo = 0x10;
                 range.Pages.emplace_back();   // an empty block
             },
             MakeError(
                 E_ARGUMENT,
                 "invalid page data: block must not be empty")},
            {[&](auto& ranges)
             {
                 {
                     auto& range = ranges.emplace_back();
                     range.FirstPageNo = 0x10;
                     range.Pages.push_back(MakeBlock(4_KB, 'A'));
                 }

                 {
                     auto& range = ranges.emplace_back();
                     range.FirstPageNo = 0x20;
                     range.Pages.push_back(MakeBlock(4_KB, 'A'));
                     range.Pages.emplace_back();   // an empty block
                     range.Pages.push_back(MakeBlock(4_KB, 'A'));
                 }
             },
             MakeError(
                 E_ARGUMENT,
                 "invalid page data: block must not be empty")},
            {[&](auto& ranges)
             {
                 auto& range = ranges.emplace_back();
                 range.FirstPageNo = 0x10;
                 range.Pages.push_back(MakeBlock(4_KB, 'A'));
                 range.Pages.push_back(MakeBlock(8_KB, 'B'));
             },
             MakeError(E_ARGUMENT, "invalid page data: block size mismatch")},
        };

        for (size_t i = 0; i != std::size(testCases); ++i) {
            const auto& [prepare, expectedError] = testCases[i];

            TVector<NJournalled::TPageRange> ranges;
            prepare(ranges);

            const auto error = WritePages(std::move(ranges));

            UNIT_ASSERT_VALUES_EQUAL_C(
                expectedError.GetCode(),
                error.GetCode(),
                "#" << (i + 1) << ": " << FormatError(expectedError) << " !~ "
                    << FormatError(error));

            UNIT_ASSERT_STRING_CONTAINS_C(
                error.GetMessage(),
                expectedError.GetMessage(),
                "#" << (i + 1) << ": " << FormatError(expectedError) << " !~ "
                    << FormatError(error));
        }
    }

    Y_UNIT_TEST_F(ShouldValidateReadPagesRequest, TFixture)
    {
        using TPrepareFunc =
            std::function<void(TVector<NJournalled::TPageRangeRef>&)>;

        const std::tuple<TPrepareFunc, NProto::TError> testCases[]{
            {[&](auto&) {}, MakeError(E_ARGUMENT, "nothing to read")},
            {[&](auto& rangeRefs) { rangeRefs.emplace_back(); },
             MakeError(
                 E_ARGUMENT,
                 "page group ref must contain at least one page")},
            {[&](auto& rangeRefs)
             {
                 rangeRefs.push_back({.FirstPageNo = 0x10, .PageCount = 1});
                 rangeRefs.push_back({.FirstPageNo = 0x20, .PageCount = 0});
             },
             MakeError(
                 E_ARGUMENT,
                 "page group ref must contain at least one page")},
        };

        for (size_t i = 0; i != std::size(testCases); ++i) {
            const auto& [prepare, expectedError] = testCases[i];

            TVector<NJournalled::TPageRangeRef> rangeRefs;
            prepare(rangeRefs);

            const auto error = ReadPages(std::move(rangeRefs)).GetError();

            UNIT_ASSERT_VALUES_EQUAL_C(
                expectedError.GetCode(),
                error.GetCode(),
                "#" << (i + 1) << ": " << FormatError(expectedError) << " !~ "
                    << FormatError(error));

            UNIT_ASSERT_STRING_CONTAINS_C(
                error.GetMessage(),
                expectedError.GetMessage(),
                "#" << (i + 1) << ": " << FormatError(expectedError) << " !~ "
                    << FormatError(error));
        }
    }

    Y_UNIT_TEST_F(ShouldServeADeviceRegion, TFixture)
    {
        constexpr ui64 firstBlock = 16;
        constexpr ui64 blockCount = 32;

        auto region = CreateDeviceAdapter(
            Timer,
            DeviceUUID,
            DefaultBlockSize,
            DeviceClient,
            {.FirstBlockIndex = firstBlock, .BlockCount = blockCount});

        FillDevice();

        const auto readPage = [&](ui64 pageNo)
        {
            return region->ReadPages({{.FirstPageNo = pageNo, .PageCount = 1}})
                .GetValueSync();
        };

        const auto assertBlock = [&](const TBuffer& page, char expected)
        {
            TStringBuf block(page.Data(), page.Size());
            UNIT_ASSERT_VALUES_EQUAL(DefaultBlockSize, block.size());
            UNIT_ASSERT_VALUES_EQUAL(
                block.size(),
                std::ranges::count(block, expected));
        };

        // page 0 of the region is the first block of the region
        {
            const auto result = readPage(0);
            UNIT_ASSERT_C(!HasError(result), FormatError(result.GetError()));
            UNIT_ASSERT_VALUES_EQUAL(1, result.GetResult().size());
            assertBlock(result.GetResult()[0], BlockData(firstBlock));
        }

        // the pages beyond the region are rejected
        {
            const auto error = readPage(blockCount).GetError();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_ARGUMENT,
                error.GetCode(),
                FormatError(error));
            UNIT_ASSERT_STRING_CONTAINS(
                error.GetMessage(),
                "beyond the device");
        }

        // a write lands in the region as well
        {
            TVector<NJournalled::TPageRange> ranges;
            auto& range = ranges.emplace_back();
            range.FirstPageNo = 1;
            range.Pages.push_back(MakeBlock(DefaultBlockSize, 'Z'));

            const auto error =
                region->WritePages(std::move(ranges)).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                FormatError(error));
        }

        {
            const auto result =
                ReadPages({{.FirstPageNo = firstBlock + 1, .PageCount = 1}});
            UNIT_ASSERT_C(!HasError(result), FormatError(result.GetError()));
            UNIT_ASSERT_VALUES_EQUAL(1, result.GetResult().size());
            assertBlock(result.GetResult()[0], 'Z');
        }

        // a write beyond the region is rejected before anything is written
        {
            TVector<NJournalled::TPageRange> ranges;
            auto& range = ranges.emplace_back();
            range.FirstPageNo = blockCount - 1;
            range.Pages.push_back(MakeBlock(DefaultBlockSize, 'Q'));
            range.Pages.push_back(MakeBlock(DefaultBlockSize, 'Q'));

            const auto error =
                region->WritePages(std::move(ranges)).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_ARGUMENT,
                error.GetCode(),
                FormatError(error));
            UNIT_ASSERT_STRING_CONTAINS(
                error.GetMessage(),
                "beyond the device");

            const ui64 lastBlock = firstBlock + blockCount - 1;
            const auto result =
                ReadPages({{.FirstPageNo = lastBlock, .PageCount = 1}});
            UNIT_ASSERT_C(!HasError(result), FormatError(result.GetError()));
            UNIT_ASSERT_VALUES_EQUAL(1, result.GetResult().size());
            assertBlock(result.GetResult()[0], BlockData(lastBlock));
        }
    }

    Y_UNIT_TEST_F(ShouldWritePages, TFixture)
    {
        const auto makeRanges = []
        {
            TVector<NJournalled::TPageRange> ranges;

            {
                auto& range = ranges.emplace_back();
                range.FirstPageNo = 0x10;
                range.Pages.push_back(MakeBlock(DefaultBlockSize, 'A'));
                range.Pages.push_back(MakeBlock(DefaultBlockSize, 'B'));
            }

            {
                auto& range = ranges.emplace_back();
                range.FirstPageNo = 0x20;
                range.Pages.push_back(MakeBlock(DefaultBlockSize, 'X'));
                range.Pages.push_back(MakeBlock(DefaultBlockSize, 'Y'));
            }

            return ranges;
        };

        {
            const auto error = WritePages(makeRanges());
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                FormatError(error));
        }
    }

    Y_UNIT_TEST_F(ShouldReadPages, TFixture)
    {
        constexpr ui32 requestCount = 100;

        FillDevice();

        for (ui32 i = 0; i != requestCount; ++i) {
            const ui64 rangeCount = 1 + RandomNumber<ui64>(8);

            TVector<NJournalled::TPageRangeRef> rangeRefs;
            for (ui64 j = 0; j != rangeCount; ++j) {
                const ui64 firstPageNo = RandomNumber<ui64>(DefaultBlockCount);

                rangeRefs.push_back(
                    {.FirstPageNo = firstPageNo,
                     .PageCount = 1 + RandomNumber<ui64>(
                                          DefaultBlockCount - firstPageNo)});
            }

            const auto result = ReadPages(rangeRefs);

            const auto& error = result.GetError();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                FormatError(error));

            // the pages of all the refs follow each other

            const auto& pages = result.GetResult();
            size_t pageIndex = 0;

            for (const auto& rangeRef: rangeRefs) {
                for (ui64 k = 0; k != rangeRef.PageCount; ++k) {
                    UNIT_ASSERT_LT(pageIndex, pages.size());

                    const ui64 blockIndex = rangeRef.FirstPageNo + k;
                    const auto& page = pages[pageIndex++];

                    TStringBuf block(page.Data(), page.Size());

                    UNIT_ASSERT_VALUES_EQUAL(DefaultBlockSize, block.size());

                    UNIT_ASSERT_VALUES_EQUAL(
                        block.size(),
                        std::ranges::count(block, BlockData(blockIndex)));
                }
            }

            UNIT_ASSERT_VALUES_EQUAL(pageIndex, pages.size());
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
