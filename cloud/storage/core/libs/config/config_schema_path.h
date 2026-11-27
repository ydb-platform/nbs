/*******************************************************************************

Schema paths for protobuf configuration traversal. The caller controls
traversal; the helper builds owning paths from field and oneof names, with []
for collections. Use these paths as keys in configuration preparation
diagnostics.

*******************************************************************************/

#pragma once

#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <google/protobuf/descriptor.h>

namespace NCloud::NConfig {

// A schema path for a caller-controlled protobuf traversal. The helper owns
// a stack of name segments; it retains neither messages nor descriptors.
// Create one helper at the root, then pair each successful Enter() with Leave()
// in the same scope, preferably through Y_DEFER { path.Leave(); }.
// Pass descriptors from the current message; their containing type is not
// checked by the helper.
// Only singular message fields allow further descent. Collections and real
// oneof groups are terminal nodes; a oneof member uses its own field name.
class TConfigSchemaPath
{
public:
    TConfigSchemaPath() = default;
    TConfigSchemaPath(const TConfigSchemaPath&) = delete;
    TConfigSchemaPath& operator=(const TConfigSchemaPath&) = delete;
    TConfigSchemaPath(TConfigSchemaPath&&) = delete;
    TConfigSchemaPath& operator=(TConfigSchemaPath&&) = delete;

    // Enter a field with [] for collections; throw if the current node is
    // terminal.
    void Enter(const google::protobuf::FieldDescriptor& field);

    // Enter a real oneof; throw for synthetic groups or below a terminal node.
    void Enter(const google::protobuf::OneofDescriptor& oneof);

    // Return to the parent; throw if already at the root.
    void Leave();

    // Build an owning, dot-separated path; return an empty string at the root.
    TString GetPath() const;

private:
    // One entered node, owned by the path stack until the matching Leave().
    struct TFrame
    {
        // Exact proto name, with [] appended for a repeated or map field.
        TString Segment;

        // Permission to enter children: true only for singular messages.
        bool CanDescend;
    };

    // Append a node after checking that the current node permits descent.
    void Enter(TString segment, bool canDescend);

private:
    // Entered nodes in root-to-leaf order; an empty stack denotes the root.
    TVector<TFrame> Frames;
};

}   // namespace NCloud::NConfig
