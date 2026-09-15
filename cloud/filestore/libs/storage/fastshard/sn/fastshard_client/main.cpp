#include <cloud/filestore/libs/storage/fastshard/sn/fastshard_client/lib/app.h>

////////////////////////////////////////////////////////////////////////////////

int main(int argc, const char* argv[])
{
    using namespace NCloud::NFileStore::NStorage::NFastShard::NClient;

    auto& app = TApp::Instance();

    ConfigureSignals();
    return app.Run(argc, argv);
}
