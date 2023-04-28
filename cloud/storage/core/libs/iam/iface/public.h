#pragma once

#include <memory>

namespace NCloud::NIamClient {

////////////////////////////////////////////////////////////////////////////////

class TIamClientConfig;
using TIamClientConfigPtr = std::shared_ptr<TIamClientConfig>;

struct IIamTokenAsyncClient;
using IIamTokenAsyncClientPtr = std::shared_ptr<IIamTokenAsyncClient>;

struct IIamTokenClient;
using IIamTokenClientPtr = std::shared_ptr<IIamTokenClient>;

}   // namespace NCloud::NIamClient
