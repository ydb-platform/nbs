#include "app.h"

#include <util/generic/yexception.h>
#include <util/stream/output.h>

////////////////////////////////////////////////////////////////////////////////

using namespace NCloud::NFileStore::NStateFileTool;

int main(int argc, char** argv)
{
    try {
        TOptions options;
        options.Parse(argc, argv);
        return AppMain(options);
    } catch (...) {
        Cerr << CurrentExceptionMessage() << Endl;
        return 1;
    }
}
