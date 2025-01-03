#pragma once

#include "error_handler.h"

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

IErrorHandlerPtr CreateErrorHandlerStub()
{
    return std::make_shared<TErrorHandlerStub>();
}

}   // namespace

}   // namespace NCloud::NBlockStore::NBD
