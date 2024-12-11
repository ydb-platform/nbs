#include "factory.h"

namespace NCloud::NFileStore::NClient {

////////////////////////////////////////////////////////////////////////////////

class TApp
{
private:
    TCommandPtr Command;

public:
    static TApp& Instance();
    int Run(int argc, char** argv);
    void Stop(int exitCode);

private:
    static TString FormatCmdLine(int argc, char** argv);
};

void Shutdown(int signum);
void ConfigureSignals();

}   // namespace NCloud::NFileStore::NClient
