#include "validate.h"

#include <cloud/blockstore/tools/testing/eternal_tests/eternal-load/lib/config.h>
#include <cloud/blockstore/tools/testing/eternal_tests/eternal-load/lib/test_executor.h>
#include <cloud/blockstore/tools/testing/eternal_tests/range-validator/lib/validate.h>

#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/generic/fwd.h>
#include <util/generic/size_literals.h>
#include <util/system/tempfile.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(ValidateTest)
{
    void ValidateRange(ui64 maxWriteRequestCount)
    {
        auto logging = CreateLoggingService("console", TLogSettings{});
        logging->Start();

        auto filePath = MakeTempName();

        auto configHolder = CreateTestConfig(
            filePath,
            1_MB, // fileSize
            1,    // ioDepth
            4096, // blockSize
            100,  // writeRage
            1,    // requestBlockCount
            1,    // writeParts
            "",   // alternatingPhase
            maxWriteRequestCount);

        auto executor = NTesting::CreateTestExecutor(
            configHolder,
            logging->CreateLog("ETERNAL_EXECUTOR")
        );
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
}

}   // namespace NCloud::NBlockStore
