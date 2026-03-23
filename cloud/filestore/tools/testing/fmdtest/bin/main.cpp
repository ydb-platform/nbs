#include "app.h"
#include "mpi.h"
#include "options.h"

#include <util/generic/yexception.h>

////////////////////////////////////////////////////////////////////////////////

int main(int argc, char** argv)
{
    using namespace NCloud::NFileStore;

    // MpiInit must come before ConfigureSignals so that MPI can install its
    // own signal handlers first; we then override them with ours.
    auto mpi = MpiInit(&argc, &argv);

    ConfigureSignals();

    TOptions options;
    try {
        options.Parse(argc, argv);
    } catch (...) {
        if (mpi.IsRoot()) {
            Cerr << CurrentExceptionMessage() << Endl;
        }
        MpiFinalize();
        return 1;
    }

    int ret = AppMain(options, mpi);

    MpiFinalize();
    return ret;
}
