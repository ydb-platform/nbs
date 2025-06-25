#pragma once

#include "public.h"

#include <exception>

namespace NCloud::NBlockStore::NBD {

////////////////////////////////////////////////////////////////////////////////

struct IErrorHandler
{
    virtual ~IErrorHandler() = default;

    virtual void ProcessException(std::exception_ptr) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IErrorHandlerMap
{
    virtual ~IErrorHandlerMap() = default;

    virtual bool Emplace(
        const TString& socket,
        IErrorHandlerPtr&& handler) = 0;

    virtual void Erase(const TString& socket) = 0;
    virtual IErrorHandlerPtr Get(const TString& socket) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IErrorHandlerPtr CreateErrorHandlerStub();
IErrorHandlerMapPtr CreateErrorHandlerMap();
IErrorHandlerMapPtr CreateErrorHandlerMapStub();

}   // namespace NCloud::NBlockStore::NBD
