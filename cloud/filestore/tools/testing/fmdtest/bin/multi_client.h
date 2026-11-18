#pragma once

#include "options.h"

#include <cloud/storage/core/libs/diagnostics/logging.h>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

/**
 * Runs a multi-client instance: Options.Rounds rounds of modifying its own
 * directory and checking the directories of the other instances, the
 * instances are synchronized via the hub.
 *
 * @param Log - Log.
 * @param options - Options.
 * @return - Process exit code: nonzero if any mismatch has been found or the
 *  synchronization has failed.
 */
int RunMultiClient(TLog& Log, const TOptions& options);

}   // namespace NCloud::NFileStore
