#pragma once

#include "public.h"

#include "grpcpp/security/server_credentials.h"

#include <memory>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

std::shared_ptr<grpc::ServerCredentials> CreateInsecureServerCredentials();

}   // namespace NCloud
