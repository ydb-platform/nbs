#include "fault_injection.h"

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/lwtrace/all.h>

#include <util/string/cast.h>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

ui32 GetCode(const NLWTrace::TCustomAction& action)
{
    if (action.OptsSize() == 0) {
        return E_FAIL;
    }

    const auto& opt = action.GetOpts(0);

    EWellKnownResultCodes code;
    if (TryFromString(opt, code)) {
        return code;
    }

    return FromString<ui32>(opt);
}

TString GetMessage(const NLWTrace::TCustomAction& action)
{
    if (action.OptsSize() > 1) {
        return action.GetOpts(1);
    }

    return {};
}

////////////////////////////////////////////////////////////////////////////////

class TServiceErrorActionExecutor final
    : public NLWTrace::TCustomActionExecutor
{
private:
    const ui32 Code;
    const TString Message;

public:
    TServiceErrorActionExecutor(
            NLWTrace::TProbe* probe,
            const NLWTrace::TCustomAction& action,
            NLWTrace::TSession* session)
        : NLWTrace::TCustomActionExecutor(probe, true /* destructive */)
        , Code(GetCode(action))
        , Message(GetMessage(action))
    {
        Y_UNUSED(probe);
        Y_UNUSED(session);
    }

private:
    bool DoExecute(NLWTrace::TOrbit& orbit, const NLWTrace::TParams& params) override
    {
        Y_UNUSED(orbit);
        Y_UNUSED(params);

        STORAGE_THROW_SERVICE_ERROR(Code) << Message;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

NLWTrace::TCustomActionExecutor* CreateServiceErrorActionExecutor(
    NLWTrace::TProbe* probe,
    const NLWTrace::TCustomAction& action,
    NLWTrace::TSession* session)
{
    return new TServiceErrorActionExecutor(probe, action, session);
}

}   // namespace NCloud::NBlockStore
