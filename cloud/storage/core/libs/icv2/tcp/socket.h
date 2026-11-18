#pragma once

#include <cloud/storage/core/libs/common/error.h>

#include <contrib/ydb/library/actors/interconnect/interconnect_stream.h>

namespace NCloud::NIcV2 {

////////////////////////////////////////////////////////////////////////////////

using TSocketPtr = TIntrusivePtr<NInterconnect::TStreamSocket>;
using TSocketResult = TResultOrError<TSocketPtr>;
using TSocketFuture = NThreading::TFuture<TSocketResult>;

}   // namespace NCloud::NIcV2
