#include <cloud/blockstore/apps/client/lib/factory.h>
#include <cloud/blockstore/tools/testing/eternal_tests/checkpoint-validator/lib/validator.h>

#include <util/stream/file.h>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TApp
{
public:
    TApp()
    {
        Logging = CreateLoggingService("console", TLogSettings{});
        Logging->Start();
        Log = Logging->CreateLog("MAIN");
    }

    static TApp& Instance()
    {
        return *Singleton<TApp>();
    }

    void Shutdown()
    {
        if (Handler) {
            Handler->Shutdown();
        }

        if (Logging) {
            Logging->Stop();
        }
    }

    int Run(int argc, const char* argv[])
    {
        if (argc <= 1) {
            STORAGE_ERROR("Not enough arguments");
            return 1;
        }

        STORAGE_INFO("Reading config");
        TString filePath(argv[1]);
        auto configHolder = LoadTestConfig(filePath);
        --argc;
        ++argv;

        auto validator = CreateValidator(
            configHolder->GetConfig(),
            Logging->CreateLog("VALIDATOR"));

        STORAGE_INFO("Starting to read all blocks");
        Handler = NClient::GetHandler("readblocks");
        Handler->SetOutputStream(validator);
        if (!Handler->Run(argc, argv)) {
            STORAGE_ERROR("Failed to read blocks");
            return 1;
        }
        STORAGE_INFO("Reading successfully finished");

        STORAGE_INFO("Starting validation")
        validator->Finish();
        if (validator->GetResult()) {
            STORAGE_INFO("Validation succeeded");
            return 0;
        } else {
            STORAGE_INFO("Validation failed");
            return 1;
        }
    }

private:
    NClient::TCommandPtr Handler;

    ILoggingServicePtr Logging;
    TLog Log;
};

void Shutdown(int signum)
{
    Y_UNUSED(signum);
    TApp::Instance().Shutdown();
}

void ConfigureSignals()
{
    std::set_new_handler(abort);

    // make sure that errors can be seen by everybody :)
    setvbuf(stdout, nullptr, _IONBF, 0);
    setvbuf(stderr, nullptr, _IONBF, 0);

    // mask signals
    signal(SIGPIPE, SIG_IGN);

    struct sigaction sa = {};
    sa.sa_handler = Shutdown;

    sigaction(SIGINT, &sa, nullptr);
    sigaction(SIGTERM, &sa, nullptr);
}

}   // namespace

}   // namespace NCloud::NBlockStore

////////////////////////////////////////////////////////////////////////////////

int main(int argc, const char* argv[])
{
    using namespace NCloud::NBlockStore;

    ConfigureSignals();
    return TApp::Instance().Run(argc, argv);
}
