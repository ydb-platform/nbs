#pragma once

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/json/json_value.h>

#include <util/generic/map.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

struct TFileAttrs
{
    ui64 Ino = 0;
    ui32 Mode = 0;
    ui64 Size = 0;
    ui64 Links = 0;
    ui64 MTimeNs = 0;
    ui64 CTimeNs = 0;

    bool operator==(const TFileAttrs& rhs) const = default;
};

/**
 * Formats the attrs for the error messages.
 *
 * @param attrs - The attrs.
 * @return - Human-readable representation.
 */
TString FormatAttrs(const TFileAttrs& attrs);

////////////////////////////////////////////////////////////////////////////////

//
// The expected state of the directory of a multi-client instance after a
// modification phase.
//

struct TDirState
{
    ui32 InstanceId = 0;
    ui32 Round = 0;

    // The attrs of the directory itself - its MTime and CTime change upon
    // each entry creation, unlink and rename.
    TFileAttrs DirAttrs;

    // The files that exist in the directory.
    TMap<TString, TFileAttrs> Files;

    // The names that have been renamed from or unlinked since the previous
    // state - they should not exist.
    TVector<TString> RenamedFrom;
    TVector<TString> Unlinked;

    /**
     * Serializes the state.
     *
     * @return - JSON representation.
     */
    NJson::TJsonValue ToJson() const;

    /**
     * Deserializes the state.
     *
     * @param json - JSON representation produced by ToJson.
     * @return - The state or an error if the JSON is malformed.
     */
    static TResultOrError<TDirState> FromJson(const NJson::TJsonValue& json);
};

}   // namespace NCloud::NFileStore
