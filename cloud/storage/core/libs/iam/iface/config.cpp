#include "config.h"

#include <library/cpp/monlib/service/pages/templates.h>

#include <chrono>

namespace NCloud::NIamClient {

using namespace std::chrono_literals;

namespace {

////////////////////////////////////////////////////////////////////////////////

#define IAM_SERVICE_CONFIG(xxx)                                                \
    xxx(MetadataServiceUrl,              TString,        {}                   )\
    xxx(TokenAgentUnixSocket,            TString,        {}                   )\
    xxx(InitialRetryTimeout,             TDuration,      500ms                )\
    xxx(RetryTimeoutIncrement,           TDuration,      0s                   )\
    xxx(RetryAttempts,                   ui32,           1                    )\
    xxx(GrpcTimeout,                     TDuration,      30s                  )\
    xxx(HttpTimeout,                     TDuration,      30s                  )\
    xxx(TokenRefreshTimeout,             TDuration,      0s                   )\
// IAM_SERVICE_CONFIG

#define IAM_SERVICE_DECLARE_CONFIG(name, type, value)                          \
    Y_DECLARE_UNUSED static const type Default##name = value;                  \
// IAM_SERVICE_DECLARE_CONFIG

IAM_SERVICE_CONFIG(IAM_SERVICE_DECLARE_CONFIG)

#undef IAM_SERVICE_DECLARE_CONFIG

////////////////////////////////////////////////////////////////////////////////

template <typename TTarget, typename TSource>
TTarget ConvertValue(TSource value)
{
    return static_cast<TTarget>(std::move(value));
}

template <>
TDuration ConvertValue<TDuration, ui32>(ui32 value)
{
    return TDuration::MilliSeconds(value);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TIamClientConfig::TIamClientConfig(NProto::TIamClientConfig config)
    : Config(std::move(config))
{}

#define IAM_SERVICE_CONFIG_GETTER(name, type, ...)                             \
type TIamClientConfig::Get##name() const                                       \
{                                                                              \
    const auto value = Config.Get##name();                                     \
    return value ? ConvertValue<type>(value) : Default##name;                  \
}                                                                              \
// IAM_SERVICE_CONFIG_GETTER

IAM_SERVICE_CONFIG(IAM_SERVICE_CONFIG_GETTER)

#undef IAM_SERVICE_CONFIG_GETTER

}   // namespace NCloud::NIamClient
