#pragma once

#include "xml_document.h"

namespace NCloud::NFileStore::NXSLRender {
    void NXSLRender(const char* xsl, const NXml::TDocument& document, IOutputStream& out);
} // namespace NCloud::NFileStore::NXSLRender
