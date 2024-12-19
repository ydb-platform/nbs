#include "xml_document.h"

#include <library/cpp/xml/document/xml-document.h>

namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NXml;
using namespace NCloud;

TDocument::Source ToTDocumentSource(TXmlNodeWrapper::ESource source)
{
    switch (source) {
        case TXmlNodeWrapper::ROOT_NAME:
            return TDocument::RootName;
        case TXmlNodeWrapper::FILE:
            return TDocument::File;
        case TXmlNodeWrapper::STRING:
            return TDocument::String;
    }
}

}   // namespace

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

struct TXmlNodeWrapper::TData
{
    TData(const TString& source, ESource type)
        : Document(std::make_shared<TDocument>(source, ToTDocumentSource(type)))
        , Node(Document->Root())
    {}

    TData toNewNode(TNode newNode) const
    {
        TData resultNode = *this;
        resultNode.Node = newNode;
        return resultNode;
    }

    std::shared_ptr<TDocument> Document;
    TNode Node;
};

TXmlNodeWrapper::TXmlNodeWrapper(const TString& source, ESource type)
    : Data(std::make_unique<TData>(source, type))
{}

TXmlNodeWrapper::~TXmlNodeWrapper()
{
    Data.~unique_ptr();
}

TXmlNodeWrapper TXmlNodeWrapper::AddChild(
    TZtStringBuf name,
    TZtStringBuf value = "")
{
    auto child = Data->Node.AddChild(name, value);
    return TXmlNodeWrapper(std::make_unique<TData>(Data->toNewNode(child)));
}

TString TXmlNodeWrapper::ToString(TZtStringBuf enc) const
{
    return Data->Node.ToString(enc);
}

TXmlNodeWrapper::TXmlNodeWrapper(std::unique_ptr<TData> data)
    : Data(std::move(data))
{}

}   // namespace NCloud
