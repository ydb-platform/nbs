#include "context.h"

#include <util/datetime/cputimer.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

TCallContext::TCallContext(ui64 requestId)
    : TCallContextBase(requestId)
{}

bool TCallContext::GetSilenceRetriableErrors() const
{
    return AtomicGet(SilenceRetriableErrors);
}

void TCallContext::SetSilenceRetriableErrors(bool silence)
{
    AtomicSet(SilenceRetriableErrors, silence);
}

bool TCallContext::GetHasUncountableRejects() const
{
    return AtomicGet(HasUncountableRejects);
}

void TCallContext::SetHasUncountableRejects()
{
    AtomicSet(HasUncountableRejects, true);
}

}   // namespace NCloud::NBlockStore
