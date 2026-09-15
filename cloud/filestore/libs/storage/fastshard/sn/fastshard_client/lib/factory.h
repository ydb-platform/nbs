#pragma once

#include "command.h"

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NFileStore::NStorage::NFastShard::NClient {

////////////////////////////////////////////////////////////////////////////////

// Lower-cases `name` and strips '-' / '_', so that "ReadPages",
// "read-pages" and "read_pages" all map to the "readpages" command.
TString NormalizeCommand(TString name);

/**
 * @param name - Normalized command name, one of GetCommandNames().
 * @param client - Storage node to run against; when null the command
 *                 connects to --host:--port itself.
 *
 * @return - The command, or null if `name` is unknown.
 */
TCommandPtr GetCommand(const TString& name, IStorageNodePtr client = {});

TVector<TString> GetCommandNames();

}   // namespace NCloud::NFileStore::NStorage::NFastShard::NClient
