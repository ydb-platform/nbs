#include <cloud/filestore/libs/diagnostics/throttler_info_serializer.h>
#include <cloud/filestore/libs/diagnostics/trace_serializer.h>
#include <cloud/filestore/libs/storage/core/public.h>
#include <cloud/filestore/libs/storage/tablet/tablet_private.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;
using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

class TAddDataActor final: public TActorBootstrapped<TAddDataActor>
{
private:
    const ITraceSerializerPtr TraceSerializer;

    const TString LogTag;
    const TActorId Tablet;
    const TRequestInfoPtr RequestInfo;

    const ui64 CommitId;
    const TVector<TMergedBlob> Blobs;
    TVector<TBlockBytesMeta> UnalignedDataParts;
    const TWriteRange WriteRange;
    ui32 BlobsSize = 0;

public:
    TAddDataActor(
        ITraceSerializerPtr traceSerializer,
        TString logTag,
        TActorId tablet,
        TRequestInfoPtr requestInfo,
        ui64 commitId,
        TVector<TMergedBlob> blobs,
        TVector<TBlockBytesMeta> unalignedDataParts,
        TWriteRange writeRange);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

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
