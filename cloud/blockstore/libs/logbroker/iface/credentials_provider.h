#pragma once

#include <memory>

////////////////////////////////////////////////////////////////////////////////

namespace NYdb::inline V3 {
class ICredentialsProviderFactory;
}   // namespace NYdb::inline V3

////////////////////////////////////////////////////////////////////////////////

namespace NCloud::NBlockStore::NLogbroker {

using NYdbICredentialsProviderFactory = NYdb::V3::ICredentialsProviderFactory;

class TLogbrokerConfig;

////////////////////////////////////////////////////////////////////////////////

std::shared_ptr<NYdbICredentialsProviderFactory>
CreateCredentialsProviderFactory(const TLogbrokerConfig& config);

}   // namespace NCloud::NBlockStore::NLogbroker
