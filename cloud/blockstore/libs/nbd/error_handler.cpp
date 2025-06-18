#include "error_handler.h"

#include <util/generic/hash.h>

#include <utility>

namespace NCloud::NBlockStore::NBD {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TErrorHandlerStub
    : IErrorHandler
{
    void ProcessException(std::exception_ptr) override
    {}
};

////////////////////////////////////////////////////////////////////////////////

struct TErrorHandlerMap
    : IErrorHandlerMap
{
    THashMap<TString, IErrorHandlerPtr> Handlers;

    bool Emplace(const TString& socket, IErrorHandlerPtr&& handler) override
    {
        return Handlers.emplace(
            socket,
            std::forward<IErrorHandlerPtr&&>(handler)).second;
    }

    void Erase(const TString& socket) override
    {
        Handlers.erase(socket);
    }

    IErrorHandlerPtr Get(const TString& socket) override
    {
        auto it = Handlers.find(socket);
        if (it != Handlers.end()) {
            return it->second;
        }
        return CreateErrorHandlerStub();
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TErrorHandlerMapStub
    : IErrorHandlerMap
{
    bool Emplace(const TString&, IErrorHandlerPtr&&) override
    {
        return true;
    }

    void Erase(const TString&) override
    {}

    IErrorHandlerPtr Get(const TString&) override
    {
        return CreateErrorHandlerStub();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IErrorHandlerPtr CreateErrorHandlerStub()
{
    return std::make_shared<TErrorHandlerStub>();
}

IErrorHandlerMapPtr CreateErrorHandlerMap()
{
    return std::make_shared<TErrorHandlerMap>();
}

IErrorHandlerMapPtr CreateErrorHandlerMapStub()
{
    return std::make_shared<TErrorHandlerMapStub>();
}

}   // namespace NCloud::NBlockStore::NBD
