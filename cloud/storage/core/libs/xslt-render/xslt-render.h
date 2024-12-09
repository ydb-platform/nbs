#pragma once

#include "xml-document.h"

#include <contrib/libs/libxml/include/libxml/globals.h>
#include <contrib/libs/libxslt/libxslt/templates.h>
#include <contrib/libs/libxslt/libxslt/transform.h>
#include <contrib/libs/libxslt/libxslt/xsltutils.h>

namespace NCloud {

class TXslRenderer final
{
public:
    explicit TXslRenderer(const char* xsl);
    ~TXslRenderer();

    void Render(const NXml::TDocument& document, IOutputStream& out);

private:
    xsltStylesheetPtr Stylesheet = nullptr;
};

}   // namespace NCloud
