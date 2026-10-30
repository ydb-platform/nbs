/*******************************************************************************

Helpers for manipulating Blockstore configuration.

*******************************************************************************/

#pragma once

#include <cloud/blockstore/config/blockstore.pb.h>

#include <cloud/storage/core/libs/common/error.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

// Remove static-only fields from dynamic configuration overrides.
// Static-only fields are set through local configuration files or command-line
// options and must never be overridden by dynamic configuration.
void RemoveStaticOnlyBlockstoreFields(NProto::TBlockstoreConfig& config);

// Normalize dynamic overrides against the prepared static configuration
// before comparing sources or building a snapshot; preserve equal static
// server/discovery ports unless discovery is explicitly overridden.
void NormalizeDynamicBlockstoreConfig(
    const NProto::TBlockstoreConfig& staticConfig,
    NProto::TBlockstoreConfig& dynamicConfig);

// Extract an unmodified config; return E_ARGUMENT with parser diagnostics for
// TError or E_INVALID_STATE for an unexpected type, without logging, so callers
// can choose how to report and handle each error.
TResultOrError<NProto::TBlockstoreConfig> ExtractBlockstoreConfig(
    const google::protobuf::Message& privateDatabaseConfig);

}   // namespace NCloud::NBlockStore
