#pragma once

#include <library/cpp/xml/document/xml-document-decl.h>

namespace NCloud::NFileStore::NXSLRender {
    void NXSLRender(const char* xsl, const NXml::TDocument& document, IOutputStream& out);
} // namespace NCloud::NFileStore::NXSLRender
