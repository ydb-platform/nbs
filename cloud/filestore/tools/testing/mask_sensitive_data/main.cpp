#include "options.h"
#include "mask.h"

#include <util/generic/yexception.h>

////////////////////////////////////////////////////////////////////////////////

int main(int argc, char** argv)
{
    using namespace NCloud::NFileStore::NMaskSensitiveData;

    TOptions options;
    try {
        options.Parse(argc, argv);
    } catch (...) {
        Cerr << CurrentExceptionMessage() << Endl;
        return 1;
    }

    TMaskSensitiveData mask{options};
    mask.MaskSensitiveData(options.InFile, options.OutFile);

    return 0;
}
