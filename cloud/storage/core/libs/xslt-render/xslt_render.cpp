#include "xslt_render.h"

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

TXslRenderer::TXslRenderer(const char* xsl)
{
    static const TXslInitializer XslInit;
    xmlDocPtr styleDoc = xmlReadDoc(BAD_CAST xsl, nullptr, "utf-8", 0);

    Stylesheet = xsltParseStylesheetDoc(styleDoc);
    if (Stylesheet == nullptr) {
        xmlFreeDoc(styleDoc);
    }
}

void TXslRenderer::Render(const NXml::TDocument& document, IOutputStream& out)
{
    auto documentStr = document.ToString("utf-8");

    xmlDocPtr sourceDoc =
        xmlReadDoc(BAD_CAST documentStr.data(), nullptr, "utf-8", 0);

    xmlDocPtr result = xsltApplyStylesheet(Stylesheet, sourceDoc, {});
    if (result != nullptr) {
        xmlChar* buffer;
        int buffer_size;

        if (!xsltSaveResultToString(&buffer, &buffer_size, result, Stylesheet))
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
    xsltFreeStylesheet(Stylesheet);
}

}   // namespace NCloud
