PROTO_LIBRARY()

ONLY_TAGS(GO_PROTO)

SRCS(
    config.proto
)

PEERDIR(
    cloud/disk_manager/internal/pkg/auth/config
    cloud/disk_manager/internal/pkg/clients/nbs/config
    cloud/disk_manager/internal/pkg/clients/nfs/config
    cloud/disk_manager/internal/pkg/dataplane/config
    cloud/disk_manager/internal/pkg/tasks/logging/config
    cloud/disk_manager/internal/pkg/monitoring/config
    cloud/disk_manager/internal/pkg/performance/config
    cloud/disk_manager/internal/pkg/persistence/config
    cloud/disk_manager/internal/pkg/services/disks/config
    cloud/disk_manager/internal/pkg/services/filesystem/config
    cloud/disk_manager/internal/pkg/services/images/config
    cloud/disk_manager/internal/pkg/services/placementgroup/config
    cloud/disk_manager/internal/pkg/services/pools/config
    cloud/disk_manager/internal/pkg/services/snapshots/config
    cloud/disk_manager/internal/pkg/tasks/config
)

END()
