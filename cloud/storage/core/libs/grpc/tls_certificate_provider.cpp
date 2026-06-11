#include "tls_certificate_provider.h"

#include "tls_utils.h"

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/yexception.h>
#include <util/stream/file.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

ICertificateProviderPtr CreateCertificateProvider(
    ILoggingServicePtr logging,
    TString logComponent,
    NMonitoring::TDynamicCountersPtr serverGroup,
    TString rootCertPath,
    TVector<TCertificateFiles> certificates,
    TDuration refreshInterval,
    ISchedulerPtr scheduler)
{
    if (refreshInterval == TDuration::Zero()) {
        return CreateStaticCertificateProvider(
            std::move(rootCertPath),
            std::move(certificates));
    }

    auto certs = NTlsUtils::PrepareAndValidateCertificates(std::move(certificates));
    if (certs.empty()) {
        return CreateCertificateProviderStub();
    }

    return CreatePeriodicCertificateProvider(
        std::move(logging),
        std::move(logComponent),
        std::move(serverGroup),
        std::move(rootCertPath),
        std::move(certs),
        refreshInterval,
        std::move(scheduler));
}

}   // namespace NCloud