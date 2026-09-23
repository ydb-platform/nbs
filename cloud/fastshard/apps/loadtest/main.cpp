#include <cloud/fastshard/apps/loadtest/lib/app.h>

////////////////////////////////////////////////////////////////////////////////

int main(int argc, const char* argv[])
{
    using namespace NCloud::NFastShard::NLoadTest;

    auto& app = TApp::Instance();

    ConfigureSignals();
    return app.Run(argc, argv);
}
