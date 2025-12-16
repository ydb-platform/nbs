#include "test.h"

#include "options.h"

#include <cloud/blockstore/tools/testing/eternal_tests/eternal-load/lib/aligned_test_scenario.h>
#include <cloud/blockstore/tools/testing/eternal_tests/eternal-load/lib/config.h>
#include <cloud/blockstore/tools/testing/eternal_tests/eternal-load/lib/test_executor.h>
#include <cloud/blockstore/tools/testing/eternal_tests/eternal-load/lib/unaligned_test_scenario.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/generic/size_literals.h>
#include <util/string/printf.h>
#include <util/system/file.h>

namespace NCloud::NBlockStore {

using namespace NTesting;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TTest final
   : public ITest
{
private:
    TOptionsPtr Options;

    ITestExecutorPtr Executor;

    IConfigHolderPtr ConfigHolder;

    ILoggingServicePtr Logging;
    TLog Log;

public:
    TTest(TOptionsPtr options)
        : Options(std::move(options))
    {
        InitLogger();
    }

    int Run() override;
    void Stop() override;

private:
    void InitLogger();

    TTestExecutorSettings ConfigureTest() const;
    int RunTest();

    void DumpConfiguration();
};

////////////////////////////////////////////////////////////////////////////////

void TTest::InitLogger()
{
    TLogSettings logSettings;
    logSettings.FiltrationLevel =
        Options->PrintDebugStats ? TLOG_DEBUG : TLOG_INFO;

    Logging = CreateLoggingService("console", logSettings);
    Logging->Start();
    Log = Logging->CreateLog("ETERNAL_MAIN");
}

int TTest::Run()
{
    STORAGE_INFO("Initializing test config");

    switch (Options->Command) {
        case ECommand::GenerateConfigCmd:
            Y_ENSURE(Options->FilePath.Defined(), "You need to specify the file path");
            Y_ENSURE(Options->FileSize.Defined(), "You need to specify the file size");
            Y_ENSURE(Options->WriteRate <= 100, "Write rate should be in range [0, 100]");

            ConfigHolder = CreateTestConfig(TCreateTestConfigArguments
                {.FilePath = *Options->FilePath,
                 .FileSize = *Options->FileSize,
                 .TestCount = Options->TestCount,
                 .IoDepth = Options->IoDepth,
                 .BlockSize = Options->BlockSize,
                 .WriteRate = Options->WriteRate,
                 .RequestBlockCount = Options->RequestBlockCount,
                 .WriteParts = Options->WriteParts,
                 .AlternatingPhase = Options->AlternatingPhase,
                 .MaxWriteRequestCount = 0,
                 .MinReadByteCount = Options->MinReadSize,
                 .MaxReadByteCount = Options->MaxReadSize,
                 .MinWriteByteCount = Options->MinWriteSize,
                 .MaxWriteByteCount = Options->MaxWriteSize,
                 .MinRegionByteCount = Options->MinRegionSize,
                 .MaxRegionByteCount = Options->MaxRegionSize});

            DumpConfiguration();
            break;
        case ECommand::ReadConfigCmd:
            Y_ENSURE(Options->RestorePath.Defined(), "You need to specify the restore path");
            ConfigHolder = LoadTestConfig(*Options->RestorePath);
            break;
        case ECommand::UnknownCmd:
            STORAGE_ERROR("Unknown command, check avaliable commands in the help");
            return 2;
    }

    return RunTest();
}

void TTest::DumpConfiguration()
{
    ConfigHolder->DumpConfig(Options->DumpPath);
    STORAGE_INFO("Test configuration and actual state have been stored to file "
        << Options->DumpPath.Quote());
}

TTestExecutorSettings TTest::ConfigureTest() const
{
    auto log = Logging->CreateLog("ETERNAL_EXECUTOR");

    TTestExecutorSettings settings;

    std::function<ITestScenarioPtr(IConfigHolderPtr)> scenarioFactory;

    switch (Options->Scenario) {
        case EScenario::Aligned:
            scenarioFactory = [&](IConfigHolderPtr config)
            {
                return CreateAlignedTestScenario(std::move(config), log);
            };
            STORAGE_INFO("Using test scenario: Aligned");
            break;

        case EScenario::Unaligned:
            scenarioFactory = [&](IConfigHolderPtr config)
            {
                return CreateUnalignedTestScenario(std::move(config), log);
            };
            STORAGE_INFO("Using test scenario: Unaligned");
            break;

        default:
            Y_ABORT("Unsupported Scenario value %d", Options->Scenario);
    }

    switch (Options->Engine) {
        case EIoEngine::AsyncIo:
            settings.FileService = ETestExecutorFileService::AsyncIo;
            STORAGE_INFO("Using file service: AsyncIo");
            break;

        case EIoEngine::IoUring:
            settings.FileService = ETestExecutorFileService::IoUring;
            STORAGE_INFO("Using file service: IoUring");
            break;

        case EIoEngine::Sync:
            settings.FileService = ETestExecutorFileService::Sync;
            STORAGE_INFO("Using file service: Sync");
            break;

        default:
            Y_ABORT("Unsupported EIoEngine value %d", Options->Engine);
    }

    TVector<IConfigHolderPtr> testConfigs;
    const auto& config = ConfigHolder->GetConfig();

    if (config.GetTestCount()) {
        const auto pos = config.GetFilePath().find("{}");
        Y_ABORT_UNLESS(
            pos != TString::npos,
            "If the test count parameter is set, the file name should contain "
            "a placeholder {}");

        const auto str1 = config.GetFilePath().substr(0, pos);
        const auto str2 = config.GetFilePath().substr(pos + 2);

        STORAGE_INFO("Using test file pattern: " << config.GetFilePath());
        STORAGE_INFO("Using test file count: " << config.GetTestCount());

        for (ui32 i = 0; i < config.GetTestCount(); i++) {
            // Tests may modify config - each test should work with own copy
            IConfigHolderPtr testConfig = ConfigHolder->Clone();
            testConfig->GetConfig().SetFilePath(
                Sprintf("%s%u%s", str1.c_str(), i, str2.c_str()));
            testConfigs.push_back(std::move(testConfig));
        }
    } else {
        STORAGE_INFO("Using test file: " << config.GetFilePath());
        testConfigs.push_back(ConfigHolder);
    }

    STORAGE_INFO("Using test file size: " << config.GetFileSize());

    for (const auto& testConfig: testConfigs) {
        settings.TestScenarios.push_back(
            {.TestScenario = scenarioFactory(testConfig),
             .FilePath = testConfig->GetConfig().GetFilePath(),
             .FileSize = testConfig->GetConfig().GetFileSize()});
    }

    settings.RunInCallbacks = Options->RunInCallbacks;
    STORAGE_INFO(
        "Using run test logic in callbacks: "
        << settings.RunInCallbacks);

    settings.NoDirect = Options->NoDirect;
    STORAGE_INFO("Using O_DIRECT: " << !settings.NoDirect);

    settings.Log = log;
    settings.PrintDebugStats = Options->PrintDebugStats;

    return settings;
}

int TTest::RunTest()
{
    auto settings = ConfigureTest();

    Executor = CreateTestExecutor(std::move(settings));

    int res = 0;
    if (!Executor->Run()) {
        STORAGE_ERROR("Test failed");
        res = 1;
    }

    DumpConfiguration();
    return res;
}

void TTest::Stop()
{
    if (Executor) {
        Executor->Stop();
    }

    if (Logging) {
        Logging->Stop();
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

ITestPtr CreateTest(TOptionsPtr options)
{
    return std::make_shared<TTest>(std::move(options));
}

}   // namespace NCloud::NBlockStore
