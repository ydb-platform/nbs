#pragma once

#include <cloud/storage/core/libs/common/error.h>

#include <util/generic/string.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

TString JsonError(const NProto::TError& e);

}   // namespace NCloud::NFileStore::NStorage
