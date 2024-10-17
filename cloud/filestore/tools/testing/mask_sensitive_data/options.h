#pragma once

#include <util/generic/string.h>

namespace NCloud::NFileStore::NMaskSensitiveData {

////////////////////////////////////////////////////////////////////////////////

struct TOptions
{
    TString InFile;
    TString OutFile;

    enum class EMode
    {
        Empty,
        Nodeid,
        Hash,
    };
    EMode Mode;

    void Parse(int argc, char** argv);
};

using TOptionsPtr = std::shared_ptr<TOptions>;

}   // namespace NCloud::NFileStore::NMaskSensitiveData
