#include "validator.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/scope.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

bool RunTest(
    ui64 size,
    ui64 iterations,
    TMaybe<ui32> brokenBlockIdx)
{
    auto logging = CreateLoggingService("console", TLogSettings{});
    logging->Start();
    Y_DEFER {
        logging->Stop();
    };

    auto configHolder = CreateTestConfig(
        "",                         //  filePath
        size * sizeof(TBlockData),  //  fileSize
        1,                          //  ioDepth
        sizeof(TBlockData),         //  blockSize
        100,                        //  writeRate
        1,                          //  requestBlockCount
        1);                         //  writeParts

    const auto& config = configHolder->GetConfig();

    auto validator = CreateValidator(
        config,
        logging->CreateLog("VALIDATOR_UT"));

    const auto& range = config.Ranges(0);
    TVector<ui64> data(size);
    ui64 cur = range.StartBlockIdx();
    for (ui32 i = 0; i < iterations; ++i) {
        data[cur] = i;
        cur = (cur + range.Step()) % size;
    }

    if (brokenBlockIdx.Defined()) {
        data[*brokenBlockIdx] -= 1;
    }

    for (const auto& value: data) {
        TBlockData blockData = {
            .RequestNumber = value,
            .BlockIndex = 0,
            .RangeIdx = 0,
            .RequestTimestamp = Now().MicroSeconds(),
            .TestTimestamp = Now().MicroSeconds(),
            .TestId = config.TestId(),
            .Checksum = 0
        };
        validator->Write(&blockData, config.BlockSize());
    }

    validator->Finish();

    return validator->GetResult();
}

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TCheckpointValidator)
{
    Y_UNIT_TEST(ShouldValidateCorrectly)
    {
        UNIT_ASSERT_VALUES_EQUAL(
            RunTest(1000, 5000, Nothing()),
            true);
    }

    Y_UNIT_TEST(ShouldFailValidation)
    {
        UNIT_ASSERT_VALUES_EQUAL(
            RunTest(1000, 5000, TMaybe<ui32>(50)),
            false);
    }
}

}   // namespace NCloud::NBlockStore
