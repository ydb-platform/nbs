#include "error.h"

#include <cstring>

namespace NCloud::NFastShard {

////////////////////////////////////////////////////////////////////////////////

TString SystemErrorText(int code)
{
    // The GNU variant: the build defines _GNU_SOURCE.
    char text[128] = {};
    return ::strerror_r(code, text, sizeof(text));
}

}   // namespace NCloud::NFastShard
