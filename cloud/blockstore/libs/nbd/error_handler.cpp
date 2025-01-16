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

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IErrorHandlerPtr CreateErrorHandlerStub()
{
    return std::make_shared<TErrorHandlerStub>();
}

}   // namespace NCloud::NBlockStore::NBD
