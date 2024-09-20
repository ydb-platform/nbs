#include "app.h"
#include "bootstrap.h"
#include "options.h"

#include <util/generic/yexception.h>

////////////////////////////////////////////////////////////////////////////////

int main(int argc, char** argv)
{
    using namespace NCloud::NFileStore::NReplay;

    ConfigureSignals();

    auto options = std::make_shared<TOptions>();
    try {
        options->Parse(argc, argv);
    } catch (...) {
        Cerr << CurrentExceptionMessage() << Endl;
        return 1;
    }

    TBootstrap bootstrap(std::move(options));
    try {
        bootstrap.Init();
        bootstrap.Start();
    } catch (...) {
        Cerr << CurrentExceptionMessage() << Endl;
        bootstrap.Stop();
        return 1;
    }

    int exitCode = AppMain(bootstrap);

    try {
        bootstrap.Stop();
    } catch (...) {
        Cerr << CurrentExceptionMessage() << Endl;
        return 1;
    }

    return exitCode;
}
