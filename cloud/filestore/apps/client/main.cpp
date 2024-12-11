#include <cloud/filestore/apps/client/lib/app.h>

////////////////////////////////////////////////////////////////////////////////

int main(int argc, char** argv)
{
    using namespace NCloud::NFileStore::NClient;

    ConfigureSignals();
    return TApp::Instance().Run(argc, argv);
}
