#include "fault_injection.h"

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/lwtrace/all.h>
#include <library/cpp/protobuf/util/pb_io.h>
#include <library/cpp/testing/unittest/registar.h>

#define UT_FAULT_INJECTION_PROVIDER(PROBE, EVENT, GROUPS, TYPES, NAMES) \
    PROBE(UtFaultInjection, GROUPS(), TYPES(ui64), NAMES("cookie"))     \
    // UT_FAULT_INJECTION_PROVIDER

LWTRACE_DECLARE_PROVIDER(UT_FAULT_INJECTION_PROVIDER);
LWTRACE_DEFINE_PROVIDER(UT_FAULT_INJECTION_PROVIDER);

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NLWTrace::TManager> CreateTraceManager()
{
    auto traceManager = std::make_unique<NLWTrace::TManager>(
        *Singleton<NLWTrace::TProbeRegistry>(),
        true);   // allowDestructiveActions

    traceManager->RegisterCustomAction(
        "ServiceErrorAction",
        &CreateServiceErrorActionExecutor);

    return traceManager;
}

NLWTrace::TQuery QueryFromString(const TString& text)
{
    TStringInput in(text);

    NLWTrace::TQuery query;
    ParseFromTextFormat(in, query);
    return query;
}

void TestFunc(int cookie)
{
    GLOBAL_LWPROBE(UT_FAULT_INJECTION_PROVIDER, UtFaultInjection, cookie);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TFaultInjectionTest)
{
    Y_UNIT_TEST(TestServiceErrorAction)
    {
        auto traceManager = CreateTraceManager();

        traceManager->New("env", QueryFromString(R"(
            Blocks {
                ProbeDesc {
                    Name: "UtFaultInjection"
                    Provider: "UT_FAULT_INJECTION_PROVIDER"
                }
                Action {
                    CustomAction {
                        Name: "ServiceErrorAction"
                        Opts: "E_REJECTED"
                        Opts: "Error!"
                    }
                }
                Predicate {
                    Operators {
                        Type: OT_EQ
                        Argument { Param: "cookie" }
                        Argument { Value: "42" }
                    }
                }
            }
        )"));

        UNIT_CHECK_GENERATED_NO_EXCEPTION(TestFunc(1), TServiceError);

        UNIT_ASSERT_EXCEPTION_SATISFIES(
            TestFunc(42),
            TServiceError,
            [](auto& e) {
                return e.GetCode() == E_REJECTED &&
                       e.GetMessage().Contains("Error!");
            });
    }
}

}   // namespace NCloud::NBlockStore
