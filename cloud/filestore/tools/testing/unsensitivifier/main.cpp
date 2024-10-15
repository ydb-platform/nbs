#include "app.h"
#include "bootstrap.h"
#include "options.h"

#include <util/generic/yexception.h>

////////////////////////////////////////////////////////////////////////////////

int main(int argc, char** argv)
{
    using namespace NCloud::NFileStore::NUnsensitivifier;

    ConfigureSignals();

    auto options = std::make_shared<TOptions>();
    try {
        options->Parse(argc, argv);
    } catch (...) {
        Cerr << CurrentExceptionMessage() << Endl;
        return 1;
    }

    TBootstrap bootstrap(std::move(options));
    int exitCode = AppMain(bootstrap);

    return exitCode;
}
