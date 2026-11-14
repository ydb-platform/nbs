/*******************************************************************************

Feature configuration composition for Blockstore sources. Apply the named
feature policy after the aggregate has been merged using protobuf rules.

*******************************************************************************/

#pragma once

#include "section_updater.h"

namespace NCloud::NBlockStore {

// Return the shared stateless Features updater with process lifetime.
const IConfigSectionUpdater& GetFeaturesSectionUpdater();

}   // namespace NCloud::NBlockStore
