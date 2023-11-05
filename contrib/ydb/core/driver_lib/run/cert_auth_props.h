#pragma once

#include <contrib/ydb/core/grpc_services/auth_processor/dynamic_node_auth_processor.h>
#include <contrib/ydb/core/protos/config.pb.h>
#include <util/generic/string.h>

namespace NKikimr {

TDynamicNodeAuthorizationParams GetDynamicNodeAuthorizationParams(const NKikimrConfig::TClientCertificateAuthorization& clientSertificateAuth);

} //namespace NKikimr
