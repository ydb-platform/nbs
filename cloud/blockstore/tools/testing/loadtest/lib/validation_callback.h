#pragma once

#include "app_context.h"

#include <cloud/blockstore/libs/validation/validation.h>

namespace NCloud::NBlockStore::NLoadTest {

////////////////////////////////////////////////////////////////////////////////

class TValidationCallback final: public IValidationCallback
{
private:
    TAppContext& AppContext;

public:
    TValidationCallback(TAppContext& appContext)
        : AppContext(appContext)
    {}

    void ReportError(const TString& message) override
    {
        Y_UNUSED(message);

        // not stopping the test immediately - letting it find and log more
        // validation errors
        AppContext.ExitCode.store(
            EC_VALIDATION_FAILED,
            std::memory_order_release);
    }
};

}   // namespace NCloud::NBlockStore::NLoadTest
