#pragma once

#include "public.h"

#include <cloud/filestore/public/api/protos/action.pb.h>

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>

#include <functional>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

using TActionHandler = std::function<
    NThreading::TFuture<NProto::TExecuteActionResponse>(const TString& input)>;

// Executes the actions in |actions| right here, e.g. the ones that need the
// daemon's own components, and forwards everything else to |service|. Action
// names are not case sensitive.
IFileStoreServicePtr CreateActionsService(
    IFileStoreServicePtr service,
    THashMap<TString, TActionHandler> actions);

// The same for the endpoint service, e.g. of filestore-vhost.
IEndpointManagerPtr CreateActionsService(
    IEndpointManagerPtr service,
    THashMap<TString, TActionHandler> actions);

// The "updatecertificates" action: |updateCertificates| re-reads the TLS
// certificate files, e.g. with ICertificateWatcher::UpdateCertificates().
TActionHandler CreateUpdateCertificatesAction(
    std::function<NThreading::TFuture<NProto::TError>()> updateCertificates);

}   // namespace NCloud::NFileStore
