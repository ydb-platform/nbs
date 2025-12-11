#include "context.h"

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

TCallContext::TCallContext(ui64 requestId)
    : TCallContext("", requestId)
{}

TCallContext::TCallContext(TString fileSystemId, ui64 requestId)
    : TCallContextBase(requestId)
    , FileSystemId(std::move(fileSystemId))
{}

TString TCallContext::LogString() const
{
    TStringBuilder sb;

    sb << GetFileStoreRequestName(RequestType) << " #" << RequestId;
    if (!FileSystemId.empty()) {
        sb << " [f:" << FileSystemId << "]";
    }

    return std::move(sb);
}

}   // namespace NCloud::NFileStore
