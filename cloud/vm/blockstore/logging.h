#pragma once

#include "private.h"

#include <cloud/vm/api/blockstore-plugin.h>

#include <library/cpp/logger/backend.h>

#include <util/generic/string.h>

namespace NCloud::NBlockStore::NPlugin {

////////////////////////////////////////////////////////////////////////////////

std::shared_ptr<TLogBackend> CreateHostLogBackend(BlockPluginHost* host);

std::shared_ptr<TLogBackend> CreateSysLogBackend(const TString& ident);

}   // namespace NCloud::NBlockStore::NPlugin
