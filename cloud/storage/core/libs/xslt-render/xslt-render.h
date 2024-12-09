#pragma once

#include <contrib/libs/libxml/include/libxml/globals.h>
#include <contrib/libs/libxslt/libxslt/templates.h>
#include <contrib/libs/libxslt/libxslt/transform.h>
#include <contrib/libs/libxslt/libxslt/xsltutils.h>

#include "xml-document.h"

namespace NCloud::NStorage::NXslRender {

class TXslRenderer
{
public:
    explicit TXslRenderer(const char* xsl);
    ~TXslRenderer();

    void Render(const NXml::TDocument& document, IOutputStream& out);
    
private:
    xsltStylesheetPtr Stylesheet = nullptr;
};

} // namespace NCloud::NStorage::NXslRender
