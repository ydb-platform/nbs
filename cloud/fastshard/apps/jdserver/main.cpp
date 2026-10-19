#include <cloud/fastshard/apps/jdserver/lib/app.h>

////////////////////////////////////////////////////////////////////////////////

int main(int argc, const char* argv[])
{
    using namespace NCloud::NFastShard::NJdServer;

    ConfigureSignals();
    return Run(argc, argv);
}
