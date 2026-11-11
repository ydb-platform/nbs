#include "app.h"
#include "bootstrap.h"

#include <cloud/fastshard/apps/jdserver/lib/options.h>

#include <util/generic/yexception.h>
#include <util/stream/output.h>
#include <util/system/progname.h>

////////////////////////////////////////////////////////////////////////////////

int main(int argc, const char* argv[])
{
    using namespace NCloud::NFastShard::NJdServer;

    ConfigureSignals();

    auto options = std::make_shared<TOptions>();
    try {
        options->Parse(argc, argv);
    } catch (...) {
        Cerr << GetProgramName() << " failed: " << CurrentExceptionMessage()
             << Endl;
        return 1;
    }

    TBootstrap bootstrap(std::move(options));
    try {
        bootstrap.Init();
        bootstrap.Start();
    } catch (...) {
        Cerr << GetProgramName() << " failed: " << CurrentExceptionMessage()
             << Endl;
        bootstrap.Stop();
        return 1;
    }

    int exitCode = AppMain(bootstrap);

    try {
        bootstrap.Stop();
    } catch (...) {
        Cerr << GetProgramName() << " failed: " << CurrentExceptionMessage()
             << Endl;
        return 1;
    }

    return exitCode;
}
