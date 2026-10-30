#include <cloud/fastshard/apps/client/lib/app.h>

////////////////////////////////////////////////////////////////////////////////

int main(int argc, const char* argv[])
{
    using namespace NCloud::NFastShard::NClient;

    auto& app = TApp::Instance();

    ConfigureSignals();
    return app.Run(argc, argv);
}
