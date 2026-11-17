#include "validate.h"

#include <cloud/blockstore/tools/testing/eternal_tests/eternal-load/lib/config.h>
#include <cloud/blockstore/tools/testing/eternal_tests/eternal-load/lib/test_executor.h>
#include <cloud/blockstore/tools/testing/eternal_tests/eternal-load/lib/test_scenarios/aligned_test_scenario.h>
#include <cloud/blockstore/tools/testing/eternal_tests/range-validator/lib/validate.h>

#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/generic/fwd.h>
#include <util/generic/size_literals.h>
#include <util/system/tempfile.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TSyncFileService final: public NTesting::ITestExecutorIOService
{
    TFile& File;

    explicit TSyncFileService(TFile& file)
        : File(file)
    {}

    void
    Read(void* buffer, ui32 count, ui64 offset, TCallback callback) override
    {
        File.Pload(buffer, count, offset);
        callback();
    }

    void Write(const void* buffer, ui32 count, ui64 offset, TCallback callback)
        override
    {
        File.Pwrite(buffer, count, offset);
        callback();
    }

    void Zero(ui32 count, ui64 offset, TCallback callback) override
    {
        Y_UNUSED(count, offset, callback);
        UNIT_FAIL("unexpected zero request");
    }

    void Stop() override
    {}

    void Fail(const TString& message) override
    {
        UNIT_FAIL(message);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(ValidateTest)
{
    void ValidateRange(ui64 maxWriteRequestCount)
    {
        auto logging = CreateLoggingService("console", TLogSettings{});
        logging->Start();

        auto filePath = MakeTempName();

        auto configHolder = CreateTestConfig(
            {.FilePath = filePath,
             .FileSize = 1_MB,
             .IoDepth = 1,
             .BlockSize = 4096,
             .WriteRate = 100,
             .RequestBlockCount = 1,
             .WriteParts = 1,
             .AlternatingPhase = "",
             .MaxWriteRequestCount = maxWriteRequestCount});

        auto log = logging->CreateLog("ETERNAL_EXECUTOR");

        auto executor = NTesting::CreateTestExecutor(
            {.TestScenarios = {{
                 .TestScenario =
                     NTesting::CreateAlignedTestScenario(configHolder, "", log),
                 .FilePath = filePath,
             }},
             .FileService = NTesting::ETestExecutorFileService::AsyncIo,
             .Log = log});

        UNIT_ASSERT(executor->Run());

        TFile file(filePath, EOpenModeFlag::RdOnly | EOpenModeFlag::DirectAligned);
        auto res = ValidateRange(file, configHolder, 0 /* rangeIdx */);
        UNIT_ASSERT_VALUES_EQUAL(0, res.InvalidBlocks.size());
    }

    Y_UNIT_TEST(ValidateRange) {
        for (int i = 0; i < 5; ++i) {
            ValidateRange(256 + i);
        }
    }

    Y_UNIT_TEST(ShouldValidateRangesWithMultiBlockRequests)
    {
        constexpr ui64 BlockSize = 32_KB;
        constexpr ui64 RequestBlockCount = 3;
        constexpr ui64 RequestCount = 16;
        constexpr ui16 IoDepth = 2;
        // Every slot is overwritten, so each holds a unique request number.
        constexpr ui64 WriteCount = RequestCount + 5;

        TTempFile tempFile(MakeTempName());
        auto configHolder = CreateTestConfig(
            {.FilePath = tempFile.Name(),
             .FileSize = IoDepth * RequestCount * RequestBlockCount * BlockSize,
             .IoDepth = IoDepth,
             .BlockSize = BlockSize,
             .WriteRate = 100,
             .RequestBlockCount = RequestBlockCount,
             .WriteParts = 1});

        {
            TFile file(
                tempFile.Name(),
                EOpenModeFlag::CreateAlways | EOpenModeFlag::RdWr);
            TSyncFileService service(file);
            auto scenario =
                NTesting::CreateAlignedTestScenario(configHolder, "", TLog());
            for (ui16 rangeIdx = 0; rangeIdx < IoDepth; ++rangeIdx) {
                for (ui64 i = 0; i < WriteCount; ++i) {
                    scenario->GetWorker(rangeIdx).Run(0, service);
                }
            }
        }

        // Replace a request number in range 1 with an overwritten one.
        const auto& range = configHolder->GetConfig().GetRanges(1);
        const ui64 corruptedRequest = WriteCount - RequestCount + 1;
        const ui64 corruptedSlot =
            (range.GetStartBlockIdx() + corruptedRequest * range.GetStep()) %
            RequestCount;
        const ui64 corruptedBlockIdx =
            range.GetStartOffset() + corruptedSlot * RequestBlockCount;
        {
            TFile file(tempFile.Name(), EOpenModeFlag::RdWr);
            TBlockData header;
            file.Pload(&header, sizeof(header), corruptedBlockIdx * BlockSize);
            UNIT_ASSERT_VALUES_EQUAL(corruptedRequest, header.RequestNumber);
            header.RequestNumber = 0;
            file.Pwrite(&header, sizeof(header), corruptedBlockIdx * BlockSize);
        }

        TFile file(
            tempFile.Name(),
            EOpenModeFlag::RdOnly | EOpenModeFlag::DirectAligned);
        TVector<TRangeValidationResult> results;
        for (ui16 rangeIdx = 0; rangeIdx < IoDepth; ++rangeIdx) {
            const auto& res = results.emplace_back(
                ValidateRange(file, configHolder, rangeIdx));
            UNIT_ASSERT_VALUES_EQUAL(
                configHolder->GetConfig().GetRanges(rangeIdx).GetStep(),
                res.GuessedStep);
            UNIT_ASSERT_VALUES_EQUAL(WriteCount - 1, res.GuessedNumberToWrite);
        }

        UNIT_ASSERT_VALUES_EQUAL(0, results[0].InvalidBlocks.size());

        UNIT_ASSERT_VALUES_EQUAL(1, results[1].InvalidBlocks.size());
        const auto& block = results[1].InvalidBlocks[0];
        UNIT_ASSERT_VALUES_EQUAL(corruptedBlockIdx, block.BlockIdx);
        UNIT_ASSERT_VALUES_EQUAL(corruptedRequest, block.ExpectedRequestNumber);
        UNIT_ASSERT_VALUES_EQUAL(0, block.ActualRequestNumber);
    }
}

}   // namespace NCloud::NBlockStore
