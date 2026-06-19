#include "app.h"
#include "options.h"

#include <util/generic/yexception.h>

#include <csignal>
#include <cstdlib>

////////////////////////////////////////////////////////////////////////////////

int main(int argc, char** argv)
{
    using namespace NCloud::NFileStore;

    if (std::getenv("FILESTORE_DIRTREE_TEST_CRASH_ON_START")) {
        std::raise(SIGSEGV);
    }

    ConfigureSignals();

    TOptions options;
    try {
        options.Parse(argc, argv);
    } catch (...) {
        Cerr << CurrentExceptionMessage() << Endl;
        return 1;
    }

    return AppMain(options);
}
