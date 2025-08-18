#include "test.h"

#include "options.h"

#include <cloud/blockstore/tools/testing/eternal_tests/eternal-load/lib/test_executor.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/generic/size_literals.h>
#include <util/system/file.h>

namespace NCloud::NBlockStore {

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
                *Options->FilePath,
                *Options->FileSize * 1_GB,
                Options->IoDepth,
                Options->BlockSize,
                Options->WriteRate,
                Options->RequestBlockCount,
                Options->WriteParts,
                Options->AlternatingPhase);
            DumpConfiguration();
            break;
        case ECommand::ReadConfigCmd:
            Y_ENSURE(Options->RestorePath.Defined(), "You need to specify the restore path");
            ConfigHolder = CreateTestConfig(*Options->RestorePath);
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

int TTest::RunTest()
{
    Executor = CreateTestExecutor(
        ConfigHolder,
        Logging->CreateLog("ETERNAL_EXECUTOR")
    );

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
