#include <cloud/blockstore/tools/cgroup-pid-writer/lib/app.h>

using namespace NCloud::NBlockStore;

////////////////////////////////////////////////////////////////////////////////

int main(int argc, char** argv)
{
    TOptions options;

    try {
        options.Parse(argc, argv);
    } catch (...) {
        Cerr << CurrentExceptionMessage() << Endl;
        return 1;
    }

    return AppMain("/sys/fs/cgroup", options);
}
