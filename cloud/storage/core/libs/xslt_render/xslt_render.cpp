#include "xslt_render.h"

#include <contrib/libs/libxml/include/libxml/globals.h>
#include <contrib/libs/libxslt/libxslt/templates.h>
#include <contrib/libs/libxslt/libxslt/transform.h>
#include <contrib/libs/libxslt/libxslt/xsltutils.h>

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TXslInitializer
{
    TXslInitializer()
    {
        xmlInitParser();
        xmlInitGlobals();
    }

    ~TXslInitializer()
    {
        xmlCleanupGlobals();
        xmlCleanupParser();
    }
};

}   // namespace

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

struct TXslRenderer::TImpl
{
    ~TImpl()
    {
        xsltFreeStylesheet(Stylesheet);
    }

    xsltStylesheetPtr Stylesheet;
};

TXslRenderer::TXslRenderer(const char* xsl)
{
    static const TXslInitializer XslInit;
    xmlDocPtr styleDoc =
        xmlReadDoc(reinterpret_cast<const xmlChar*>(xsl), nullptr, "utf-8", 0);

    Impl = std::make_unique<TImpl>(xsltParseStylesheetDoc(styleDoc));
    if (Impl->Stylesheet == nullptr) {
        xmlFreeDoc(styleDoc);
    }
}

TXslRenderer::~TXslRenderer() = default;

int TXslRenderer::Render(const TXmlNodeWrapper& document, IOutputStream& out)
{
    auto documentStr = document.ToString("utf-8");

    xmlDocPtr sourceDoc = xmlReadDoc(
        reinterpret_cast<const xmlChar*>(documentStr.data()),
        nullptr,
        "utf-8",
        0);

    int returnCode = 0;
    xmlDocPtr result = xsltApplyStylesheet(Impl->Stylesheet, sourceDoc, {});
    if (result != nullptr) {
        xmlChar* buffer = nullptr;
        int bufferSize = 0;

        if (!xsltSaveResultToString(
                &buffer,
                &bufferSize,
                result,
                Impl->Stylesheet))
        {
            out << reinterpret_cast<char*>(buffer);
        } else {
            returnCode = -2;
        }
        xmlFree(buffer);
    } else {
        returnCode = -1;
    }

    xmlFreeDoc(result);
    xmlFreeDoc(sourceDoc);
    return returnCode;
}

}   // namespace NCloud
