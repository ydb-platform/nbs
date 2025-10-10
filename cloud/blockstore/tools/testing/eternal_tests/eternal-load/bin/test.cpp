#include "test.h"

#include "options.h"

#include <cloud/blockstore/tools/testing/eternal_tests/eternal-load/lib/aligned_test_scenario.h>
#include <cloud/blockstore/tools/testing/eternal_tests/eternal-load/lib/config.h>
#include <cloud/blockstore/tools/testing/eternal_tests/eternal-load/lib/test_executor.h>
#include <cloud/blockstore/tools/testing/eternal_tests/eternal-load/lib/unaligned_test_scenario.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/generic/size_literals.h>
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
    Logging = CreateLoggingService("console", TLogSettings{});
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

            ConfigHolder = CreateTestConfig(
                {.FilePath = *Options->FilePath,
                 .FileSize = *Options->FileSize * 1_GB,
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

    switch (Options->Scenario) {
        case EScenario::Aligned:
            settings.TestScenario =
                CreateAlignedTestScenario(ConfigHolder, log);
            STORAGE_INFO("Using test scenario: Aligned");
            break;

        case EScenario::Unaligned:
            settings.TestScenario =
                CreateUnalignedTestScenario(ConfigHolder, log);
            STORAGE_INFO("Using test scenario: Unaligned");
            break;

        default:
            Y_ABORT("Unsupported Scenario value %d", Options->Engine);
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

    settings.FilePath = ConfigHolder->GetConfig().GetFilePath();
    STORAGE_INFO("Using test file: " << settings.FilePath);

    settings.FileSize = ConfigHolder->GetConfig().GetFileSize();
    STORAGE_INFO("Using test file size: " << settings.FileSize);

    settings.RunInCallbacks = Options->RunInCallbacks;
    STORAGE_INFO(
        "Using run test logic in callbacks: "
        << settings.RunInCallbacks);

    settings.NoDirect = Options->NoDirect;
    STORAGE_INFO("Using O_DIRECT: " << !settings.NoDirect);

    settings.Log = log;

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
