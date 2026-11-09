#pragma once

#include "public.h"

#include "node_ref.h"

#include <contrib/ydb/library/actors/core/actorid.h>

#include <util/datetime/base.h>
#include <util/digest/multi.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/vector.h>

#include <memory>
#include <utility>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

/**
 * In-memory registry of the clients (identified by the tablet pipe servers
 * they are connected through) that have read Nodes, NodeRefs and NodeXAttrs
 * and therefore need to receive FileSystemEvents when those Nodes, NodeRefs
 * and NodeXAttrs change.
 *
 * The registrations are kept in two generations: the active one and the
 * sealed one. New registrations go to the active generation. Once per seal
 * period the sealed generation is dropped, the active one is sealed and a new
 * active one is created - this bounds the memory consumption. Lookups consult
 * both generations.
 *
 * Within a generation each client gets an index in the vector of the unique
 * clients of the generation, and the clients of a Node, a NodeRef or a
 * NodeXAttr are stored as a paged bitmap of these indices. Each page is a ui64
 * covering 64 client indices. The first page is stored inline, the other
 * pages are created on demand - only for the pages that have bits set.
 *
 * Extracting the subscribers of a Node, a NodeRef or a NodeXAttr removes it
 * from both generations: a client that receives an invalidation drops the
 * cached data and registers again upon the next read.
 */
class TFileSystemEventSubscriptions
{
public:
    struct TStats
    {
        // The number of the Nodes registered in both generations.
        ui64 NodeCount;

        // The number of the NodeRefs registered in both generations.
        ui64 NodeRefCount;

        // The number of the unique clients of both generations (a client
        // present in both generations is counted twice).
        ui64 ClientCount;

        // The number of extra pages stored in all generations.
        ui64 ExtraPageCount;

        // The number of the extended attributes registered in both generations.
        ui64 NodeXAttrCount;
    };

private:
    struct TNodeXAttrKey
    {
        ui64 NodeId;
        TString Name;

        TNodeXAttrKey(ui64 nodeId, TString name)
            : NodeId(nodeId)
            , Name(std::move(name))
        {}

        [[nodiscard]] size_t GetHash() const
        {
            return MultiHash(NodeId, Name);
        }

        bool operator==(const TNodeXAttrKey& rhs) const = default;
    };

    struct TNodeXAttrKeyHash
    {
        size_t operator()(const TNodeXAttrKey& key) const noexcept
        {
            return key.GetHash();
        }
    };

    class TClientMask
    {
    private:
        static constexpr ui32 PageSize = 64;

        struct TPage
        {
            ui32 PageNo = 0;
            ui64 Bits = 0;
        };

        // Client indices 0..63.
        ui64 FirstPage = 0;
        // Client indices >= 64, created on demand.
        std::unique_ptr<TVector<TPage>> OtherPages;

    public:
        [[nodiscard]] ui32 Set(ui32 clientIndex);

        template <typename TFunc>
        void ForEach(TFunc&& func) const;

        [[nodiscard]] ui32 GetExtraPageCount() const;

    private:
        template <typename TFunc>
        static void ForEachInPage(ui64 bits, ui32 base, TFunc& func);
    };

    class TGeneration
    {
    private:
        TVector<NActors::TActorId> Clients;
        THashMap<NActors::TActorId, ui32> ClientIndices;

        THashMap<ui64, TClientMask> NodeMasks;
        THashMap<TNodeRefKey, TClientMask, TNodeRefKeyHash> NodeRefMasks;
        THashMap<TNodeXAttrKey, TClientMask, TNodeXAttrKeyHash>
            NodeXAttrMasks;

        ui64 ExtraPageCount = 0;

    public:
        void AddNode(ui64 nodeId, const NActors::TActorId& pipeServerId);
        void AddNodeRef(
            const TNodeRefKey& key,
            const NActors::TActorId& pipeServerId);
        void AddNodeXAttr(
            const TNodeXAttrKey& key,
            const NActors::TActorId& pipeServerId);

        void ExtractNode(ui64 nodeId, THashSet<NActors::TActorId>& out);
        void ExtractNodeRef(
            const TNodeRefKey& key,
            THashSet<NActors::TActorId>& out);
        void ExtractNodeXAttr(
            const TNodeXAttrKey& key,
            THashSet<NActors::TActorId>& out);

        [[nodiscard]] TStats GetStats() const;

    private:
        ui32 GetClientIndex(const NActors::TActorId& pipeServerId);
        void CollectClients(
            const TClientMask& mask,
            THashSet<NActors::TActorId>& out) const;

        template <typename TMasks>
        void Extract(
            TMasks& masks,
            const typename TMasks::key_type& key,
            THashSet<NActors::TActorId>& out);
    };

    const TDuration SealPeriod;

    TGeneration Active;
    TGeneration Sealed;
    TInstant LastSealTs;

public:
    /**
     * @param sealPeriod - How often the active generation is sealed and the
     * sealed one is dropped. Should be greater than the client-side cache
     * timeouts: a registration lives between one and two seal periods.
     */
    explicit TFileSystemEventSubscriptions(TDuration sealPeriod);

    /**
     * Registers a client as a reader of a Node.
     *
     * @param nodeId - Node identifier.
     * @param pipeServerId - Pipe server the client is connected through.
     * @param now - Current time, used for the generation rotation.
     */
    void AddNodeSubscriber(
        ui64 nodeId,
        const NActors::TActorId& pipeServerId,
        TInstant now);

    /**
     * Registers a client as a reader of a NodeRef.
     *
     * @param parentNodeId - Parent directory identifier.
     * @param name - Entry name.
     * @param pipeServerId - Pipe server the client is connected through.
     * @param now - Current time, used for the generation rotation.
     */
    void AddNodeRefSubscriber(
        ui64 parentNodeId,
        const TString& name,
        const NActors::TActorId& pipeServerId,
        TInstant now);

    /**
     * Registers a client as a reader of an extended attribute of a Node.
     *
     * @param nodeId - Node identifier.
     * @param name - Extended attribute name.
     * @param pipeServerId - Pipe server the client is connected through.
     * @param now - Current time, used for the generation rotation.
     */
    void AddNodeXAttrSubscriber(
        ui64 nodeId,
        const TString& name,
        const NActors::TActorId& pipeServerId,
        TInstant now);

    /**
     * Extracts the clients to be notified about a Node change and removes the
     * Node from both generations.
     *
     * @param nodeId - Node identifier.
     * @param now - Current time, used for the generation rotation.
     * @return - Pipe servers of the clients, without duplicates.
     */
    TVector<NActors::TActorId> ExtractNodeSubscribers(
        ui64 nodeId,
        TInstant now);

    /**
     * Extracts the clients to be notified about a NodeRef change and removes
     * the NodeRef from both generations.
     *
     * @param parentNodeId - Parent directory identifier.
     * @param name - Entry name.
     * @param now - Current time, used for the generation rotation.
     * @return - Pipe servers of the clients, without duplicates.
     */
    TVector<NActors::TActorId> ExtractNodeRefSubscribers(
        ui64 parentNodeId,
        const TString& name,
        TInstant now);

    /**
     * Extracts the clients to be notified about an extended attribute change
     * and removes the extended attribute from both generations.
     *
     * @param nodeId - Node identifier.
     * @param name - Extended attribute name.
     * @param now - Current time, used for the generation rotation.
     * @return - Pipe servers of the clients, without duplicates.
     */
    TVector<NActors::TActorId> ExtractNodeXAttrSubscribers(
        ui64 nodeId,
        const TString& name,
        TInstant now);

    /**
     * @return - The seal period the registry was created with.
     */
    [[nodiscard]] TDuration GetSealPeriod() const;

    /**
     * @return - TStats describing this subscription registry.
     */
    [[nodiscard]] TStats GetStats() const;

private:
    void SealIfNeeded(TInstant now);
};

}   // namespace NCloud::NFileStore::NStorage
