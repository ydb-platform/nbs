#include "config.h"

#include <library/cpp/monlib/service/pages/templates.h>

namespace NCloud::NIamClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

TDuration Minutes(ui64 x)
{
    return TDuration::Minutes(x);
}

TDuration Seconds(ui64 x)
{
    return TDuration::Seconds(x);
}

TDuration MSeconds(ui64 x)
{
    return TDuration::MilliSeconds(x);
}

////////////////////////////////////////////////////////////////////////////////

#define IAM_SERVICE_CONFIG(xxx)                        \
    xxx(MetadataServiceUrl, TString, {})               \
    xxx(TokenAgentUnixSocket, TString, {})             \
    xxx(InitialRetryTimeout, TDuration, MSeconds(500)) \
    xxx(RetryTimeoutIncrement, TDuration, MSeconds(0)) \
    xxx(RetryAttempts, ui32, 1)                        \
    xxx(GrpcTimeout, TDuration, Seconds(30))           \
    xxx(HttpTimeout, TDuration, Seconds(30))           \
    xxx(TokenRefreshTimeout, TDuration, Minutes(0))    \
    // IAM_SERVICE_CONFIG

#define IAM_SERVICE_DECLARE_CONFIG(name, type, value)         \
    Y_DECLARE_UNUSED static const type Default##name = value; \
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

#define IAM_SERVICE_CONFIG_GETTER(name, type, ...)                \
    type TIamClientConfig::Get##name() const                      \
    {                                                             \
        const auto value = Config.Get##name();                    \
        return value ? ConvertValue<type>(value) : Default##name; \
    }                                                             \
    // IAM_SERVICE_CONFIG_GETTER

IAM_SERVICE_CONFIG(IAM_SERVICE_CONFIG_GETTER)

#undef IAM_SERVICE_CONFIG_GETTER

}   // namespace NCloud::NIamClient
