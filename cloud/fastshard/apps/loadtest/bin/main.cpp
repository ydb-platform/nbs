#include "app.h"
#include "bootstrap.h"
#include "options.h"

#include <silk/util/logger.h>

#include <util/generic/yexception.h>

////////////////////////////////////////////////////////////////////////////////

int main(int argc, const char* argv[])
{
    using namespace NCloud::NFastShard::NLoadTest;

    ConfigureSignals();

    auto options = std::make_shared<TOptions>();
    try {
        options->Parse(argc, argv);
    } catch (...) {
        SILK_ERROR("%s", CurrentExceptionMessage().c_str());
        return 1;
    }

    TBootstrap bootstrap(std::move(options));
    try {
        bootstrap.Init();
        bootstrap.Start();
    } catch (...) {
        SILK_ERROR("%s", CurrentExceptionMessage().c_str());
        bootstrap.Stop();
        return 1;
    }

    int exitCode = AppMain(bootstrap);

    try {
        bootstrap.Stop();
    } catch (...) {
        SILK_ERROR("%s", CurrentExceptionMessage().c_str());
        return 1;
    }

    return exitCode;
}
