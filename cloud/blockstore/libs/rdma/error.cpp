#include "error.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>

namespace NCloud::NBlockStore::NRdma {

////////////////////////////////////////////////////////////////////////////////

size_t SerializeError(ui32 code, TStringBuf message, TStringBuf buffer)
{
    auto error = MakeError(code, TString(message));

    if (error.ByteSizeLong() > buffer.size()) {
        error.SetMessage("rdma error");
        ReportFailedToSerializeRdmaError();
    }

    if (error.SerializeToArray(
        const_cast<char*>(buffer.data()),
        error.ByteSize()))
    {
        return error.ByteSizeLong();
    }

    return 0;   // will be interpreted as E_FAIL by ParseError
}

NProto::TError ParseError(TStringBuf buffer)
{
    NProto::TError error;

    if (!error.ParseFromArray(buffer.data(), buffer.size())) {
        error.SetCode(E_FAIL);
        error.SetMessage("rdma error");
        ReportFailedToParseRdmaError();
    }

    return error;
}

}   // namespace NCloud::NBlockStore::NRdma
