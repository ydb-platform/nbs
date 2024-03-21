#include <cloud/filestore/libs/diagnostics/throttler_info_serializer.h>
#include <cloud/filestore/libs/diagnostics/trace_serializer.h>
#include <cloud/filestore/libs/storage/core/public.h>
#include <cloud/filestore/libs/storage/tablet/tablet_private.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;
using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

class TWriteDataActor final: public TActorBootstrapped<TWriteDataActor>
{
private:
    const ITraceSerializerPtr TraceSerializer;

    const TString LogTag;
    const TActorId Tablet;
    const TRequestInfoPtr RequestInfo;

    const ui64 CommitId;
    /*const*/ TVector<TMergedBlob> Blobs;
    const TWriteRange WriteRange;
    ui32 BlobsSize = 0;

    // This parameter is used for a scenario, when there is already a blobId
    // with data written to. In this case, we don't need to send requests to
    // blobstorage. This field is used only for two-stage writes. For more info
    // see See #539
    bool SkipBlobStorage = false;

public:
    TWriteDataActor(
        ITraceSerializerPtr traceSerializer,
        TString logTag,
        TActorId tablet,
        TRequestInfoPtr requestInfo,
        ui64 commitId,
        TVector<TMergedBlob> blobs,
        TWriteRange writeRange,
        bool skipBlobStorage);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void WriteBlob(const TActorContext& ctx);
    void HandleWriteBlobResponse(
        const TEvIndexTabletPrivate::TEvWriteBlobResponse::TPtr& ev,
        const TActorContext& ctx);

    void AddBlob(const TActorContext& ctx);
    void HandleAddBlobResponse(
        const TEvIndexTabletPrivate::TEvAddBlobResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void
    ReplyAndDie(const TActorContext& ctx, const NProto::TError& error = {});
};

}   // namespace NCloud::NFileStore::NStorage
