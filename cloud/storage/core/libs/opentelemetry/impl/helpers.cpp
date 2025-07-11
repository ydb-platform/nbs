#include "helpers.h"

#include <util/generic/string.h>

#include <iomanip>
#include <iostream>
#include <sstream>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

TString ToHexString8(ui64 value)
{
    // std::stringstream is used here, because StringStream from util doesn't
    // support std::hex, std::setw, std::setfill.
    std::stringstream buf;
    buf << std::hex << std::setw(8) << std::setfill('0') << value;
    return buf.str();
}

TString ToHexString16(ui64 value)
{
    // std::stringstream is used here, because StringStream from util doesn't
    // support std::hex, std::setw, std::setfill.
    std::stringstream buf;
    buf << std::hex << std::setw(16) << std::setfill('0') << value;
    return buf.str();
}

}   // namespace NCloud
