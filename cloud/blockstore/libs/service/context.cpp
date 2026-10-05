#include "context.h"

#include <util/datetime/cputimer.h>
#include <util/system/yassert.h>

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

void TCallContext::SetLatencyDiagnostics(
    NProto::TLatencyDiagnostics diagnostics)
{
    auto data = std::make_shared<const NProto::TLatencyDiagnostics>(
        std::move(diagnostics));
    std::lock_guard lock(LatencyLock);
    LatencyDiagnostics = std::move(data);
}

std::shared_ptr<const NProto::TLatencyDiagnostics>
TCallContext::GetLatencyDiagnostics() const
{
    std::lock_guard lock(LatencyLock);
    return LatencyDiagnostics;
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
