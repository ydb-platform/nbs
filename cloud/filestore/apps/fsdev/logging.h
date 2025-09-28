#pragma once

#include "private.h"

#include <library/cpp/logger/backend.h>

namespace NCloud::NFileStore::NFsdev {

////////////////////////////////////////////////////////////////////////////////

std::shared_ptr<TLogBackend> CreateSpdkLogBackend();

}   // namespace namespace NCloud::NFileStore::NFsdev
