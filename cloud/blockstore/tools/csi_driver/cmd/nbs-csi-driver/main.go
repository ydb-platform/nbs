package main

import (
	"flag"
	"log"

	"github.com/ydb-platform/nbs/cloud/blockstore/tools/csi_driver/internal/driver"
)

////////////////////////////////////////////////////////////////////////////////

func main() {
	cfg := driver.Config{}

	flag.StringVar(&cfg.DriverName, "name", "nbs.csi.driver", "Driver name")
	flag.StringVar(&cfg.VendorVersion, "version", "devel", "Vendor version")
	flag.StringVar(&cfg.Endpoint, "endpoint", "/csi/csi.sock", "CSI endpoint")
	flag.StringVar(&cfg.NodeID, "node-id", "undefined", "Node ID")
	flag.BoolVar(&cfg.VMMode, "vm-mode", false, "Pass socket files to containers for VMs")
	flag.UintVar(&cfg.MonPort, "mon-port", 8774, "Monitoring port")
	flag.StringVar(&cfg.NbsHost, "nbs-host", "localhost", "NBS host")
	flag.UintVar(&cfg.NbsPort, "nbs-port", 9766, "NBS port")
	flag.StringVar(&cfg.NbsSocket, "nbs-socket", "", "NBS unix socket path")
	flag.StringVar(&cfg.NfsServerHost, "nfs-server-host", "localhost", "NFS server host")
	flag.UintVar(&cfg.NfsServerPort, "nfs-server-port", 0, "NFS server port")
	flag.StringVar(&cfg.NfsServerSocket, "nfs-server-socket", "", "NFS server unix socket path")
	flag.StringVar(&cfg.NfsVhostHost, "nfs-vhost-host", "localhost", "NFS vhost host")
	flag.UintVar(&cfg.NfsVhostPort, "nfs-vhost-port", 0, "NFS vhost port")
	flag.StringVar(&cfg.NfsVhostSocket, "nfs-vhost-socket", "", "NFS vhost unix socket path")
	flag.StringVar(&cfg.SocketsDir, "sockets-dir", "/run/nbsd/sockets", "Path to folder with disk sockets")
	flag.StringVar(&cfg.LocalFilestoreOverridePath, "local-filestore-override", "", "Path to file with local filestore override entries")
	flag.StringVar(&cfg.NfsLocalHost, "nfs-local-host", "localhost", "NFS local host")
	flag.UintVar(&cfg.NfsLocalFilestorePort, "nfs-local-filestore-port", 0, "NFS local filestore port")
	flag.StringVar(&cfg.NfsLocalFilestoreSocket, "nfs-local-filestore-socket", "", "NFS local filestore unix socket path")
	flag.UintVar(&cfg.NfsLocalEndpointPort, "nfs-local-endpoint-port", 0, "NFS local endpoint port")
	flag.StringVar(&cfg.NfsLocalEndpointSocket, "nfs-local-endpoint-socket", "", "NFS local endpoint unix socket path")
	flag.StringVar(&cfg.MountOptions, "mount-options", "grpid", "Comma-separated list of filesystem mount options")

	flag.Parse()

	log.Printf("Run NBS CSI driver: %s:%s", cfg.DriverName, cfg.VendorVersion)

	srv, err := driver.NewDriver(cfg)
	if err != nil {
		panic(err)
	}
	if err := srv.Run(cfg.Endpoint); err != nil {
		panic(err)
	}
}
