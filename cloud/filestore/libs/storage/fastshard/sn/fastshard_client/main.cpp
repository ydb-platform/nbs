#include <cloud/filestore/libs/storage/fastshard/sn/fastshard_client/lib/app.h>

////////////////////////////////////////////////////////////////////////////////

int main(int argc, const char* argv[])
{
    using namespace NCloud::NFileStore::NStorage::NFastShard::NClient;

    ConfigureSignals();
    return TApp::Instance().Run(argc, argv);
}
