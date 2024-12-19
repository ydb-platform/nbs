#pragma once

#include "xml_document.h"

#include <util/stream/output.h>

#include <memory.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

class TXslRenderer final
{
public:
    explicit TXslRenderer(const char* xsl);

    ~TXslRenderer();

    int Render(const TXmlNodeWrapper& document, IOutputStream& out);

private:
    struct TImpl;

    std::unique_ptr<TImpl> Impl;
};

}   // namespace NCloud
