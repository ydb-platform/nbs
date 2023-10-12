#pragma once

#include "public.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request.h>

#include <cloud/storage/core/protos/media.pb.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <array>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

using TIncompleteRequestsCollector = std::function<void(
    TCallContext& callContext,
    IVolumeInfoPtr volumeInfo,
    NCloud::NProto::EStorageMediaKind mediaKind,
    EBlockStoreRequest requestType,
    TRequestTime time)>;

////////////////////////////////////////////////////////////////////////////////

struct IIncompleteRequestProvider
{
    virtual ~IIncompleteRequestProvider() = default;

    virtual size_t CollectRequests(
        const TIncompleteRequestsCollector& collector) = 0;
};

IIncompleteRequestProviderPtr CreateIncompleteRequestProviderStub();

TIncompleteRequestsCollector CreateIncompleteRequestsCollectorStub();

}   // namespace NCloud::NBlockStore
