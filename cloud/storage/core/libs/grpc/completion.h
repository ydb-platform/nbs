#pragma once

#include "public.h"

#include "grpcpp/completion_queue.h"

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

bool EnqueueCompletion(::grpc::CompletionQueue* completionQueue, void* tag);

}   // namespace NCloud
