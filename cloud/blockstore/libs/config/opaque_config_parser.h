/*******************************************************************************

The Blockstore parser for the PrivateDatabaseConfig YAML section.
Startup registration and ConfigsDispatcher use the same callback to obtain a
TBlockstoreConfig or a TError with parser diagnostics. Callers handle section
absence and report errors; the parser receives only the section contents and
does not log.

*******************************************************************************/

#pragma once

#include <contrib/ydb/core/config/init/init.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

// Create a PrivateDatabaseConfig parser accepting empty input as an empty
// config and returning NCloud::NProto::TError with parser diagnostics on
// failure.
NKikimr::NConfig::TOpaqueConfigParser CreateBlockstoreOpaqueConfigParser();

}   // namespace NCloud::NBlockStore
