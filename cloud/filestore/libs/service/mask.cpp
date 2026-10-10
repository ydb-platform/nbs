#include "mask.h"

#include <library/cpp/digest/md5/md5.h>

#include <util/string/builder.h>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

TString MaskFileName(const TString& name)
{
    TStringBuf sbuf(name);
    size_t pos = sbuf.rfind('.');
    const ui32 maxExtensionLength = 4;
    if (pos == 0 || pos + 1 + maxExtensionLength < sbuf.size()) {
        pos = TString::npos;
    }
    TStringBuilder maskedName;
    maskedName << MD5::Calc(sbuf.substr(0, pos));
    if (pos != TString::npos) {
        maskedName << sbuf.substr(pos);
    }
    return maskedName;
}

}   // namespace NCloud::NFileStore
