#pragma once

#include "public.h"

#include <cloud/filestore/public/api/protos/filesystem_event.pb.h>

#include <util/generic/string.h>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

struct IFileSystemEventHandler
{
    virtual ~IFileSystemEventHandler() = default;

    /**
     * Called for each FileSystemEvent received from the storage. Called from
     * the transport thread, must not block.
     *
     * @param event - The event.
     */
    virtual void OnEvent(const NProto::TFileSystemEvent& event) = 0;

    /**
     * Called when the transport that delivers the events of a source gets
     * disconnected. Events sent by this source before the disconnect may have
     * been lost.
     *
     * @param sourceId - Identifier of the source, the same as the
     *  *CacheSourceId values in the responses whose data this source
     *  invalidates.
     */
    virtual void OnDisconnect(ui64 sourceId) = 0;

    /**
     * Tells whether the events are consumed by anybody. If not, the responses
     * don't need to be stamped with the source ids.
     *
     * A consumer has to be registered before it issues the requests whose
     * responses it caches - otherwise these responses may be left unstamped.
     * An unstamped response is still safe to cache: its data is attributed to
     * UnknownSourceId and is invalidated upon the disconnect of any source.
     *
     * @return - True if the events are consumed.
     */
    virtual bool HasConsumers() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

//
// Handler that forwards OnEvent to the handlers registered for the event's
// FileSystemId and OnDisconnect - to all the registered handlers. It has
// consumers while any handler is registered.
//

struct IMultiFileSystemEventHandler
    : public IFileSystemEventHandler
{
    /**
     * Registers a handler. OnEvent is delivered to the handlers registered
     * for the event's FileSystemId, OnDisconnect - to all the handlers.
     *
     * @param fileSystemId - Main FileSystem identifier.
     * @param handler - Handler to register.
     */
    virtual void Register(
        const TString& fileSystemId,
        IFileSystemEventHandlerPtr handler) = 0;

    /**
     * Unregisters a handler previously registered via Register.
     *
     * @param fileSystemId - Main FileSystem identifier.
     * @param handler - Handler to unregister.
     */
    virtual void Unregister(
        const TString& fileSystemId,
        const IFileSystemEventHandlerPtr& handler) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IMultiFileSystemEventHandlerPtr CreateMultiFileSystemEventHandler();

}   // namespace NCloud::NFileStore
