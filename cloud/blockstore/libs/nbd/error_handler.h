#pragma once

#include "public.h"

namespace NCloud::NBlockStore::NBD {

////////////////////////////////////////////////////////////////////////////////

struct IErrorHandler
{
    virtual ~IErrorHandler() = default;

    virtual void ProcessException(std::exception_ptr) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IErrorHandlerPtr CreateErrorHandlerStub();

}   // namespace NCloud::NBlockStore::NBD
