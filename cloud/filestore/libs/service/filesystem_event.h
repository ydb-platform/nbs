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
     * Called when the transport that delivers the events gets disconnected.
     * Events sent before the disconnect may have been lost.
     *
     * @param tabletId - the id of the disconnected tablet (can be used for
     * diagnostic purposes).
     */
    virtual void OnDisconnect(ui64 tabletId) = 0;
};

////////////////////////////////////////////////////////////////////////////////

//
// Handler that forwards OnEvent to the handlers registered for the event's
// FileSystemId and OnDisconnect - to all the registered handlers.
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
