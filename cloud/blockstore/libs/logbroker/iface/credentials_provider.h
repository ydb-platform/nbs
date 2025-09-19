#pragma once

#include <memory>

////////////////////////////////////////////////////////////////////////////////

namespace NYdb {
    class ICredentialsProviderFactory;
}   // namespace NYdb

////////////////////////////////////////////////////////////////////////////////

namespace NCloud::NBlockStore::NLogbroker {

using NYdbICredentialsProviderFactory = NYdb::ICredentialsProviderFactory;

class TLogbrokerConfig;

////////////////////////////////////////////////////////////////////////////////

std::shared_ptr<NYdbICredentialsProviderFactory>
CreateCredentialsProviderFactory(const TLogbrokerConfig& config);

}   // namespace NCloud::NBlockStore::NLogbroker
