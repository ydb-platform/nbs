#pragma once

#include "xml_document.h"
#include <memory.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

class TXslRenderer final
{
public:
    explicit TXslRenderer(const char* xsl);
    ~TXslRenderer();

    void Render(const NXml::TDocument& document, IOutputStream& out);

private:
    struct TData;

    std::unique_ptr<TData> Stylesheet = nullptr;
};

}   // namespace NCloud
