#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/context.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct TCallContext final: public TCallContextBase
{
private:
    TCallContext* Parent = nullptr;
    TAtomic SilenceRetriableErrors = false;
    TAtomic HasUncountableRejects = false;

public:
    TCallContext(ui64 requestId = 0, TCallContextPtr parent = {});

    TCallContextPtr CreateChild(ui32 fork, ui64 nowCycles);

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

TCallContextPtr ToBlockStoreCallContext(TCallContextBasePtr callContext);

}   // namespace NCloud::NBlockStore
