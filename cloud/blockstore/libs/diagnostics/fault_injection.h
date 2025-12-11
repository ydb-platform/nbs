#pragma once

#include "public.h"

namespace NLWTrace {
class TCustomAction;
class TCustomActionExecutor;
class TSession;
struct TProbe;
}   // namespace NLWTrace

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

NLWTrace::TCustomActionExecutor* CreateServiceErrorActionExecutor(
    NLWTrace::TProbe* probe,
    const NLWTrace::TCustomAction& action,
    NLWTrace::TSession* session);

}   // namespace NCloud::NBlockStore
