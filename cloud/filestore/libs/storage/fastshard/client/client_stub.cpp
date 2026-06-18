#include "client.h"

#include <cloud/storage/core/libs/common/error.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

using namespace NProtoSrv;

////////////////////////////////////////////////////////////////////////////////

std::shared_ptr<IEndpoint> TClient::Connect(const TString& host, ui16 port)
{
    Y_UNUSED(host, port);
    return nullptr;
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
