#include "device_path.h"

#include <library/cpp/uri/uri.h>

#include <util/string/builder.h>

namespace NCloud::NBlockStore {

NProto::TError DevicePath::Parse(const TString& devicePath)
{
    // rdma://myt1-ct5-13.cloud.yandex.net:10020/62ccf40f2743308191205ad6391bfa06

    NUri::TUri uri;
    auto res = uri.Parse(
        devicePath,
        NUri::TFeature::FeaturesDefault |
            NUri::TFeature::FeatureSchemeFlexible);
    if (res != NUri::TState::ParsedOK) {
        return MakeError(
            E_FAIL,
            TStringBuilder() << "invalid device path " << devicePath);
    }

    if (uri.GetField(NUri::TField::FieldScheme) != Protocol) {
        return MakeError(
            E_FAIL,
            TStringBuilder() << "device path doesn't start with " << Protocol
                             << "://, " << devicePath);
    }

    auto path = uri.GetField(NUri::TField::FieldPath);
    if (path.size() < 2 || path[0] != '/') {
        return MakeError(
            E_FAIL,
            TStringBuilder()
                << "invalid uuid inside device path " << devicePath);
    }

    Host = uri.GetHost();
    Port = uri.GetPort();
    Uuid = path.substr(1);

    return {};
}

TString DevicePath::Serialize() const
{
    return TStringBuilder()
           << Protocol << "://" << Host << ":" << Port << "/" << Uuid;
}

}   // namespace NCloud::NBlockStore
