#include "incomplete_requests.h"

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

TIncompleteRequest::TIncompleteRequest(
    NCloud::NProto::EStorageMediaKind mediaKind,
    EFileStoreRequest requestType,
    TDuration executionTime,
    TDuration totalTime)
    : MediaKind(mediaKind)
    , RequestType(requestType)
    , ExecutionTime(executionTime)
    , TotalTime(totalTime)
{}

}   // namespace NCloud::NFileStore
