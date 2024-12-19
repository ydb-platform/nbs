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

struct TXslRenderer::TData
{
    xsltStylesheetPtr Value;
};

TXslRenderer::TXslRenderer(const char* xsl)
{
    static const TXslInitializer XslInit;
    xmlDocPtr styleDoc = xmlReadDoc(BAD_CAST xsl, nullptr, "utf-8", 0);

    Stylesheet = std::make_unique<TData>(xsltParseStylesheetDoc(styleDoc));
    if (Stylesheet->Value == nullptr) {
        xmlFreeDoc(styleDoc);
    }
}

void TXslRenderer::Render(const TXmlNodeWrapper& document, IOutputStream& out)
{
    auto documentStr = document.ToString("utf-8");

    xmlDocPtr sourceDoc =
        xmlReadDoc(BAD_CAST documentStr.data(), nullptr, "utf-8", 0);

    xmlDocPtr result = xsltApplyStylesheet(Stylesheet->Value, sourceDoc, {});
    if (result != nullptr) {
        xmlChar* buffer = nullptr;
        int bufferSize = 0;

        if (!xsltSaveResultToString(
                &buffer,
                &bufferSize,
                result,
                Stylesheet->Value))
        {
            out << (char*)buffer;
        } else {
            out << "Error returning page";
        }
        xmlFree(buffer);
    } else {
        out << "Error rendering page: " << documentStr;
    }

    xmlFreeDoc(result);
    xmlFreeDoc(sourceDoc);
}

TXslRenderer::~TXslRenderer()
{
    xsltFreeStylesheet(Stylesheet->Value);
}

}   // namespace NCloud
