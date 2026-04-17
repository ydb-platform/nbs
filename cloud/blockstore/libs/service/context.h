#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/context.h>

#include <util/system/yassert.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct TCallContext final
    : public TCallContextBase
{
private:
    TAtomic SilenceRetriableErrors = false;
    TAtomic HasUncountableRejects = false;

public:
    TCallContext(ui64 requestId = 0);

    bool GetSilenceRetriableErrors() const;
    void SetSilenceRetriableErrors(bool silence);

    bool GetHasUncountableRejects() const;
    void SetHasUncountableRejects();
};

////////////////////////////////////////////////////////////////////////////////

inline TCallContextPtr CreateCallContext(ui64 requestId = 0)
{
    return MakeIntrusive<TCallContext>(requestId);
}

////////////////////////////////////////////////////////////////////////////////

inline TCallContextPtr ToBlockStoreCallContext(TCallContextBasePtr callContext)
{
    if (!callContext) {
        return {};
    }

    auto concrete = std::move(callContext).As<TCallContext>();
    Y_ABORT_UNLESS(concrete);
    return concrete;
}

}   // namespace NCloud::NBlockStore
