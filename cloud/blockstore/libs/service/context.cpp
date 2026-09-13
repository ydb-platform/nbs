#include "context.h"

#include <util/datetime/cputimer.h>
#include <util/system/yassert.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

TCallContext::TCallContext(ui64 requestId, TCallContextPtr parent)
    : TCallContextBase(requestId, parent)
    , Parent(parent.Get())
{}

TCallContextPtr TCallContext::CreateChild(ui32 fork, ui64 nowCycles)
{
    if (!IsRequestTimingEnabled()) {
        return TCallContextPtr(this);
    }
    auto child = MakeIntrusive<TCallContext>(RequestId, TCallContextPtr(this));
    child->InitChildRequestTiming(TCallContextBasePtr(this), fork, nowCycles);
    return child;
}

bool TCallContext::GetSilenceRetriableErrors() const
{
    if (Parent) {
        return Parent->GetSilenceRetriableErrors();
    }

    return AtomicGet(SilenceRetriableErrors);
}

void TCallContext::SetSilenceRetriableErrors(bool silence)
{
    if (Parent) {
        Parent->SetSilenceRetriableErrors(silence);
        return;
    }

    AtomicSet(SilenceRetriableErrors, silence);
}

bool TCallContext::GetHasUncountableRejects() const
{
    if (Parent) {
        return Parent->GetHasUncountableRejects();
    }

    return AtomicGet(HasUncountableRejects);
}

void TCallContext::SetHasUncountableRejects()
{
    if (Parent) {
        Parent->SetHasUncountableRejects();
        return;
    }

    AtomicSet(HasUncountableRejects, true);
}

TCallContextPtr ToBlockStoreCallContext(TCallContextBasePtr callContext)
{
    if (!callContext) {
        return {};
    }

    auto* concrete = dynamic_cast<TCallContext*>(callContext.Get());
    Y_ABORT_UNLESS(concrete);
    return TCallContextPtr(concrete);
}

}   // namespace NCloud::NBlockStore
