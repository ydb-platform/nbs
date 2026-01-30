#pragma once

#include <cloud/blockstore/public/api/protos/headers.pb.h>

namespace NCloud {

namespace Impl {

template <typename T>
concept HasHeaders = requires(T t) {
    { t.GetHeaders() } -> std::same_as<const NBlockStore::NProto::THeaders&>;
};

}   // namespace Impl

template <typename T>
TString SafeDebugPrint(const T& message)
{
    if constexpr (Impl::HasHeaders<T>) {
        if (message.HasHeaders() && message.GetHeaders().HasInternal() &&
            !message.GetHeaders().GetInternal().GetAuthToken().empty())
        {
            T copy = message;
            copy.MutableHeaders()->MutableInternal()->SetAuthToken("xxx");
            return copy.ShortDebugString();
        }
    }
    return message.ShortDebugString();
}

}   // namespace NCloud
