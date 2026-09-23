#include <cloud/fastshard/loadtest/lib/app.h>

////////////////////////////////////////////////////////////////////////////////

int main(int argc, const char* argv[])
{
    using namespace NCloud::NFastShard::NLoadTest;

    // Construct the singleton before a signal handler can reach it:
    // Singleton<> takes a lock and registers an atexit destroyer on first
    // use, neither of which is safe from a signal handler.
    auto& app = TApp::Instance();

    ConfigureSignals();
    return app.Run(argc, argv);
}
