#pragma once

#include <cloud/filestore/libs/storage/fastshard/sn/iface/storage_node.h>

#include <memory>

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

/**
 * Storage group iface. Right now it's basically the same iface as IStorageNode
 * but it's better to keep a separate declaration to highlight that these things
 * are actually different entities. Storage groups are supposed to provide
 * some extra non-functional features on top of multiple storage nodes - like
 * redundancy or hedged requests.
 *
 * The interface is synchronous and is supposed to be used from a fiber.
 */
struct IStorageGroup
{
    virtual ~IStorageGroup() = default;

#define SN_DECLARE_METHOD(name, ...)                                           \
    virtual NProto::T##name##Response name(                                    \
        NProto::T##name##Request request) = 0;                                 \
// SN_DECLARE_METHOD

    SN_METHODS(SN_DECLARE_METHOD)

#undef SN_DECLARE_METHOD
};

using IStorageGroupPtr = std::shared_ptr<IStorageGroup>;

/**
 * Returns an IStorageGroup which mirrors each write into all storage nodes and
 * reads from one of the nodes selecting it in a round-robin manner. The
 * implementation is naive in the sense that it does no crash recovery and is
 * basically a happy-path implementation intended for tests and prototyping. And
 * there's also no real m/n write / k/n read quorum here - it's just always
 * n/n for writes, 1/n for reads.
 *
 * @return - The constructed group.
 */
IStorageGroupPtr CreateNaiveMirroredStorageGroup(
    TVector<IStorageNodePtr> nodes);

}   // namespace NCloud::NFileStore::NStorage::NFastShard
