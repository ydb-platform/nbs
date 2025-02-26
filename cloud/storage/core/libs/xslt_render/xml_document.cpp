#include "xml_document.h"

#include <library/cpp/xml/document/xml-document.h>

namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NXml;
using namespace NCloud;

TDocument::Source ToDocumentSource(TXmlNodeWrapper::ESource source)
{
    switch (source) {
        case TXmlNodeWrapper::ESource::ROOT_NAME:
            return TDocument::RootName;
        case TXmlNodeWrapper::ESource::FILE:
            return TDocument::File;
        case TXmlNodeWrapper::ESource::STRING:
            return TDocument::String;
    }
}

}   // namespace

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

struct TXmlNodeWrapper::TImpl
{
    TImpl(const TString& source, ESource type)
        : Document(std::make_shared<TDocument>(source, ToDocumentSource(type)))
        , Node(Document->Root())
    {}

    TImpl ToNewNode(TNode newNode) const
    {
        TImpl resultNode = *this;
        resultNode.Node = newNode;
        return resultNode;
    }

    std::shared_ptr<TDocument> Document;
    TNode Node;
};

TXmlNodeWrapper::TXmlNodeWrapper(const TString& source, ESource type)
    : Impl(std::make_unique<TImpl>(source, type))
{}

TXmlNodeWrapper::~TXmlNodeWrapper() = default;

TString TXmlNodeWrapper::ToString(TString encoding) const
{
    return Impl->Node.ToString(std::move(encoding));
}

TXmlNodeWrapper::TXmlNodeWrapper(std::unique_ptr<TImpl> impl)
    : Impl(std::move(impl))
{}

TXmlNodeWrapper TXmlNodeWrapper::AddChildImpl(TString tag, TString content = "")
{
    auto child = Impl->Node.AddChild(std::move(tag), std::move(content));
    return TXmlNodeWrapper(std::make_unique<TImpl>(Impl->ToNewNode(child)));
}

}   // namespace NCloud
