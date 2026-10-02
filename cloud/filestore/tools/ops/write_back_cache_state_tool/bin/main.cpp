#include "app.h"

#include <util/generic/yexception.h>

////////////////////////////////////////////////////////////////////////////////

using namespace NCloud::NFileStore::NWriteBackCacheStateTool;

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
