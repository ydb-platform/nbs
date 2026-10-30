#pragma once

#include "public.h"
#include "tls_utils.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/public.h>
#include <cloud/storage/core/libs/common/startable.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/threading/future/core/future.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>

#include <functional>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

// Reads, validates, refreshes and reports certificate files for the whole
// process. The same files watched twice share one entry: one read per check
// and one set of metrics.
struct ICertificateWatcher
    : IStartable
{
    using TIdentityCallback =
        std::function<void(const NTlsUtils::TIdentityContent&)>;
    using TRootCaCallback = std::function<void(const TString&)>;

    // Load the files right away and throw if that fails. |onChange| is called
    // from the update task after new content has been applied.
    virtual NTlsUtils::TIdentityContent WatchIdentity(
        const TCertificateFiles& files,
        TIdentityCallback onChange) = 0;

    virtual TString WatchRootCa(
        const TString& path,
        TRootCaCallback onChange) = 0;

    // Re-reads the files right away and applies new content that passes
    // validation, bypassing the stable read done by periodic checks: call it
    // once the files have been written completely. Returns the first read or
    // validation error, E_TRY_AGAIN if another update is pending or in
    // progress and E_INVALID_STATE if the watcher is not started. The
    // callbacks of the future must not wait for another update.
    virtual NThreading::TFuture<NProto::TError> UpdateCertificates() = 0;
};

////////////////////////////////////////////////////////////////////////////////

// Zero |refreshInterval| loads the files once and never checks them again.
ICertificateWatcherPtr CreateCertificateWatcher(
    ILoggingServicePtr logging,
    TString logComponent,
    ISchedulerPtr scheduler,
    ITaskQueuePtr taskQueue,
    NMonitoring::TDynamicCountersPtr counters,
    TDuration refreshInterval);

}   // namespace NCloud
