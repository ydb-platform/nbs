#include "bootstrap.h"

#include "options.h"

namespace NCloud::NFileStore::NUnsensitivifier {

////////////////////////////////////////////////////////////////////////////////

TBootstrap::TBootstrap(TOptionsPtr options)
    : Options(std::move(options))
{}

TBootstrap::~TBootstrap()
{}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NFileStore::NUnsensitivifier
