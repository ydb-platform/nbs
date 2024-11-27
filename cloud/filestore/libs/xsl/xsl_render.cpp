#include "xsl_render.h"

#include <contrib/libs/libxml/include/libxml/globals.h>
#include <contrib/libs/libxslt/libxslt/templates.h>
#include <contrib/libs/libxslt/libxslt/transform.h>
#include <contrib/libs/libxslt/libxslt/xsltutils.h>

namespace {
struct TXslInitializer {
    TXslInitializer() {
        xmlInitParser();
        xmlInitGlobals();
    }

    ~TXslInitializer() {
        xmlCleanupGlobals();
        xmlCleanupParser();
    }
};

TXslInitializer xslInit;
}  // namespace

void NCloud::NFileStore::NXSLRender::NXSLRender(const char* xsl, const NXml::TDocument& document, IOutputStream& out) {
    auto document_str = document.ToString("utf-8");

    xmlDocPtr sourceDoc = xmlReadDoc(
        BAD_CAST
        document_str.data(),
        nullptr, nullptr, 0
    );
    xmlDocPtr styleDoc = xmlReadDoc(
        BAD_CAST
        xsl,
        nullptr, nullptr, 0
    );

    xsltStylesheetPtr style = xsltParseStylesheetDoc(styleDoc);
    
    xmlDocPtr result = xsltApplyStylesheet(style, sourceDoc, {});

    xmlChar* buffer;
    int buffer_size;

    if (!xsltSaveResultToString(&buffer, &buffer_size, result, style)) {
        out << (char*)buffer;
    } else {
        out << "Error rendering page";
    }

    xsltFreeStylesheet(style);
	xmlFreeDoc(result);
	xmlFreeDoc(sourceDoc);
    xmlFree(buffer);
}
