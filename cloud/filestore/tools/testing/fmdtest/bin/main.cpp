#include "app.h"
#include "multi_client.h"
#include "options.h"
#include "sync.h"

#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/logger/stream.h>

#include <util/generic/yexception.h>

////////////////////////////////////////////////////////////////////////////////

int main(int argc, char** argv)
{
    using namespace NCloud::NFileStore;

    ConfigureSignals();

    TOptions options;
    try {
        options.Parse(argc, argv);
    } catch (...) {
        Cerr << CurrentExceptionMessage() << Endl;
        return 1;
    }

    if (options.Mode == EMode::Bench) {
        return AppMain(options);
    }

    auto asyncLogger = NCloud::CreateAsyncLogger();
    asyncLogger->Start();
    // capital 'L' letter needed for the logging macros
    TLog Log = NCloud::CreateComponentLog(
        options.Mode == EMode::SyncHub ? "HUB" : "CLIENT",
        std::make_shared<TStreamLogBackend>(&Cerr),
        asyncLogger);

    const int result = options.Mode == EMode::SyncHub
        ? RunSyncHub(
            Log,
            options.SyncPort,
            options.InstanceCount,
            options.SyncTimeout)
        : RunMultiClient(Log, options);

    asyncLogger->Stop();
    return result;
}
