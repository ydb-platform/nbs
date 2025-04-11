#include "xslt_render.h"

#include <contrib/libs/libxml/include/libxml/globals.h>
#include <contrib/libs/libxslt/libxslt/templates.h>
#include <contrib/libs/libxslt/libxslt/transform.h>
#include <contrib/libs/libxslt/libxslt/xsltutils.h>

#include <util/generic/scope.h>

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

NProto::TError TXslRenderer::Render(
    const TXmlNodeWrapper& document,
    IOutputStream& out)
{
    auto documentStr = document.ToString("utf-8");

    xmlDocPtr sourceDoc = xmlReadDoc(
        reinterpret_cast<const xmlChar*>(documentStr.data()),
        nullptr,
        "utf-8",
        0);
    Y_DEFER {
        xmlFreeDoc(sourceDoc);
    };

    xmlDocPtr result = xsltApplyStylesheet(Impl->Stylesheet, sourceDoc, {});
    Y_DEFER {
        xmlFreeDoc(result);
    };
    if (result != nullptr) {
        xmlChar* buffer = nullptr;
        Y_DEFER {
            xmlFree(buffer);
        };
        int bufferSize = 0;

        if (!xsltSaveResultToString(
                &buffer,
                &bufferSize,
                result,
                Impl->Stylesheet))
        {
            out << reinterpret_cast<char*>(buffer);
        } else {
            return MakeError(E_FAIL, "Unable to serialize XSLT result");
        }
    } else {
        return MakeError(E_FAIL, "Unable to apply stylesheet");
    }

    return {};
}

}   // namespace NCloud
