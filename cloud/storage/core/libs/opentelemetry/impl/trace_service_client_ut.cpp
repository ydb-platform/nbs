#include "trace_service_client.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/opentelemetry/iface/trace_service_client.h>

#include <library/cpp/lwtrace/all.h>
#include <library/cpp/lwtrace/protos/lwtrace.pb.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/random/random.h>

#include <opentelemetry/proto/collector/trace/v1/trace_service.pb.h>

namespace NCloud {

using namespace opentelemetry::proto::collector::trace::v1;

using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TraceServiceClientTest)
{
    Y_UNIT_TEST(ShouldNotCrashWithFailedRequests)
    {
        auto logging = CreateLoggingService("console");
        NProto::TGrpcClientConfig clientConfig;
        auto port = RandomNumber<ui64>(65535 - 1025) + 1025;
        clientConfig.SetAddress("localhost:" + ToString(port));
        clientConfig.SetInsecure(true);
        clientConfig.SetRequestTimeout(1);

        auto client = CreateTraceServiceClient(logging, clientConfig);

        client->Start();
        Y_DEFER
        {
            client->Stop();
        };

        auto testDuration = TDuration::Seconds(10);
        TVector<TFuture<TResultOrError<ExportTraceServiceResponse>>> results;

        auto start = TInstant::Now();

        while (TInstant::Now() - start < testDuration) {
            results.push_back(client->Export(ExportTraceServiceRequest(), ""));
        }

        for (auto& result: results) {
            UNIT_ASSERT(HasError(result.GetValueSync()));
        }
    }
}

}   // namespace NCloud
