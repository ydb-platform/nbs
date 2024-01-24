#include "config.h"
#include <util/generic/fwd.h>
#include <util/generic/size_literals.h>
#include <util/random/random.h>
#include <util/system/tempfile.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(ConfigTest)
{
    const TString expectedConfig = R"(
        {
        "FileSize":10737418240,
        "IoDepth":12,
        "Ranges":
            [
            {
                "NumberToWrite":0,
                "Step":215455,
                "RequestBlockCount":1,
                "WriteParts":1,
                "LastBlockIdx":0,
                "StartOffset":0,
                "RequestCount":218453,
                "StartBlockIdx":0
            },
            {
                "NumberToWrite":0,
                "Step":7383,
                "RequestBlockCount":1,
                "WriteParts":1,
                "LastBlockIdx":1,
                "StartOffset":218453,
                "RequestCount":218453,
                "StartBlockIdx":1
            },
            {
                "NumberToWrite":0,
                "Step":128369,
                "RequestBlockCount":1,
                "WriteParts":1,
                "LastBlockIdx":2,
                "StartOffset":436906,
                "RequestCount":218453,
                "StartBlockIdx":2
            },
            {
                "NumberToWrite":0,
                "Step":129175,
                "RequestBlockCount":1,
                "WriteParts":1,
                "LastBlockIdx":3,
                "StartOffset":655359,
                "RequestCount":218453,
                "StartBlockIdx":3
            },
            {
                "NumberToWrite":0,
                "Step":127960,
                "RequestBlockCount":1,
                "WriteParts":1,
                "LastBlockIdx":4,
                "StartOffset":873812,
                "RequestCount":218453,
                "StartBlockIdx":4
            },
            {
                "NumberToWrite":0,
                "Step":114861,
                "RequestBlockCount":1,
                "WriteParts":1,
                "LastBlockIdx":5,
                "StartOffset":1092265,
                "RequestCount":218453,
                "StartBlockIdx":5
            },
            {
                "NumberToWrite":0,
                "Step":189869,
                "RequestBlockCount":1,
                "WriteParts":1,
                "LastBlockIdx":6,
                "StartOffset":1310718,
                "RequestCount":218453,
                "StartBlockIdx":6
            },
            {
                "NumberToWrite":0,
                "Step":45612,
                "RequestBlockCount":1,
                "WriteParts":1,
                "LastBlockIdx":7,
                "StartOffset":1529171,
                "RequestCount":218453,
                "StartBlockIdx":7
            },
            {
                "NumberToWrite":0,
                "Step":141828,
                "RequestBlockCount":1,
                "WriteParts":1,
                "LastBlockIdx":8,
                "StartOffset":1747624,
                "RequestCount":218453,
                "StartBlockIdx":8
            },
            {
                "NumberToWrite":0,
                "Step":185274,
                "RequestBlockCount":1,
                "WriteParts":1,
                "LastBlockIdx":9,
                "StartOffset":1966077,
                "RequestCount":218453,
                "StartBlockIdx":9
            },
            {
                "NumberToWrite":0,
                "Step":84122,
                "RequestBlockCount":1,
                "WriteParts":1,
                "LastBlockIdx":10,
                "StartOffset":2184530,
                "RequestCount":218453,
                "StartBlockIdx":10
            },
            {
                "NumberToWrite":0,
                "Step":115403,
                "RequestBlockCount":1,
                "WriteParts":1,
                "LastBlockIdx":11,
                "StartOffset":2402983,
                "RequestCount":218453,
                "StartBlockIdx":11
            }
            ],
        "BlockSize":4096,
        "FilePath":"/dev/vdb",
        "WriteRate":50,
        "RangeBlockCount":218453,
        "TestId":13930160852258120406
        }
    )";
    
    Y_UNIT_TEST(ConfigParserWorksAsExpected)
    {
        auto filename = MakeTempName();
        {
            TFileOutput output(filename);
            output.Write(expectedConfig);
        }

        auto configHolder = CreateTestConfig(filename);
        auto& config = configHolder->GetConfig();
        UNIT_ASSERT_EQUAL(config.GetFilePath(), "/dev/vdb");
        UNIT_ASSERT_EQUAL(config.GetFileSize(), 10 * 1_GB);
        UNIT_ASSERT_EQUAL(config.GetIoDepth(), 12);
        UNIT_ASSERT_EQUAL(config.GetBlockSize(), 4096);
        UNIT_ASSERT_EQUAL(config.GetWriteRate(), 50);
        UNIT_ASSERT_EQUAL(config.GetRangeBlockCount(), 218453);
        UNIT_ASSERT_EQUAL(config.GetAlternatingPhase(), "");
        UNIT_ASSERT_EQUAL(config.GetTestId(), 13930160852258120406ull);
        UNIT_ASSERT_EQUAL(config.GetRanges().size(), 12);
        UNIT_ASSERT_EQUAL(config.GetRanges(11).GetStartOffset(), 2402983);
        UNIT_ASSERT_EQUAL(config.GetRanges(11).GetRequestCount(), 218453);
        UNIT_ASSERT_EQUAL(config.GetRanges(11).GetRequestBlockCount(), 1);
        UNIT_ASSERT_EQUAL(config.GetRanges(11).GetStep(), 115403);
        UNIT_ASSERT_EQUAL(config.GetRanges(11).GetStartBlockIdx(), 11);
        UNIT_ASSERT_EQUAL(config.GetRanges(11).GetLastBlockIdx(), 11);
        UNIT_ASSERT_EQUAL(config.GetRanges(11).GetNumberToWrite(), 0);
        UNIT_ASSERT_EQUAL(config.GetRanges(11).GetWriteParts(), 1);
    }
    
    Y_UNIT_TEST(ConfigGenerationWorksAsExpected)
    {
        NJson::TJsonValue expectedConfigJson;
        UNIT_ASSERT(NJson::ReadJsonTree(expectedConfig, &expectedConfigJson, true));

        // Test config generation is not deterministic, so we need to set random seed
        SetRandomSeed(42);

        auto configHolder = CreateTestConfig("/dev/vdb", 10 * 1_GB, 12, 4096, 50, 1, 1);
        auto filename = MakeTempName();
        configHolder->DumpConfig(filename);

        TFileInput input(filename);

        NJson::TJsonValue actualConfigJson;
        UNIT_ASSERT(NJson::ReadJsonTree(&input, &actualConfigJson, true));

        UNIT_ASSERT_EQUAL(expectedConfigJson, actualConfigJson);
    }
}

}   // namespace NCloud::NBlockStore
