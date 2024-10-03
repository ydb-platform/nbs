#include "request.h"

// TODO(proller): will be in next PR

namespace NCloud::NFileStore::NLoadTest {
IRequestGeneratorPtr CreateReplayRequestGeneratorGRPC(
    NProto::TReplaySpec /*spec*/,
    ILoggingServicePtr /*logging*/,
    NClient::ISessionPtr /*session*/,
    TString /*filesystemId*/,
    NProto::THeaders /*headers*/)
{
    return {};
}

}   // namespace NCloud::NFileStore::NLoadTest
