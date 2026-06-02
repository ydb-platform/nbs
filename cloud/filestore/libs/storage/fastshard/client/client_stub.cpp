#include "client.h"

#include <cloud/storage/core/libs/common/error.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

using namespace NProtoSrv;

////////////////////////////////////////////////////////////////////////////////

TClient::TClient(ui16 port)
    : Port(port)
{
    Y_UNUSED(Port);
}

TResponse TClient::Send(const TRequest& req)
{
    Y_UNUSED(req);

    TResponse resp;
    *resp.MutableError() = MakeError(E_NOT_IMPLEMENTED);
    return resp;
}

int TClient::Connect()
{
    return -1;
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
