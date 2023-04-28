#include <cloud/blockstore/client/lib/app.h>

////////////////////////////////////////////////////////////////////////////////

int main(int argc, const char* argv[])
{
    using namespace NCloud::NBlockStore::NClient;

    ConfigureSignals();
    return TApp::Instance().Run(argc, argv);
}
