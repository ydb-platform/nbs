#pragma once

#include <memory>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct IEncryptor;
using IEncryptorPtr = std::shared_ptr<IEncryptor>;

}   // namespace NCloud::NBlockStore
