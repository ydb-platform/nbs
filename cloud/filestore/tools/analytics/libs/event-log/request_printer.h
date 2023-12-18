#pragma once

#include "public.h"

#include <util/generic/fwd.h>

namespace NCloud::NFileStore {

namespace NProto {

////////////////////////////////////////////////////////////////////////////////

class TProfileLogRequestInfo;

}   // namespace NProto

////////////////////////////////////////////////////////////////////////////////

class IRequestPrinter
{
public:
    virtual ~IRequestPrinter() = default;

    virtual TString DumpInfo(
        const NProto::TProfileLogRequestInfo& request) const = 0;
};

////////////////////////////////////////////////////////////////////////////////

IRequestPrinterPtr CreateRequestPrinter(ui32 requestType);

}   // namespace NCloud::NFileStore
