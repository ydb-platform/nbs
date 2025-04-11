#pragma once

#include "xml_document.h"

#include <cloud/storage/core/libs/common/error.h>

#include <util/stream/output.h>

#include <memory.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

class TXslRenderer final
{
public:
    explicit TXslRenderer(const char* xsl);

    ~TXslRenderer();

    NProto::TError Render(const TXmlNodeWrapper& document, IOutputStream& out);

private:
    struct TImpl;

    std::unique_ptr<TImpl> Impl;
};

}   // namespace NCloud
