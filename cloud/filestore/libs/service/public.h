#pragma once

#include <util/generic/ptr.h>

#include <memory>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

struct TCallContext;
using TCallContextPtr = TIntrusivePtr<TCallContext>;

template <typename T>
struct IResponseHandler;

template <typename T>
using IResponseHandlerPtr = std::shared_ptr<IResponseHandler<T>>;

struct IFileStore;
using IFileStorePtr = std::shared_ptr<IFileStore>;

struct IFileStoreService;
using IFileStoreServicePtr = std::shared_ptr<IFileStoreService>;

struct IFileStoreEndpoints;
using IFileStoreEndpointsPtr = std::shared_ptr<IFileStoreEndpoints>;

struct IEndpointManager;
using IEndpointManagerPtr = std::shared_ptr<IEndpointManager>;

struct IAuthProvider;
using IAuthProviderPtr = std::shared_ptr<IAuthProvider>;

}   // namespace NCloud::NFileStore
