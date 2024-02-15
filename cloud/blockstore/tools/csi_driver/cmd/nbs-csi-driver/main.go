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
	flag.UintVar(&cfg.NbsPort, "nbs-port", 9766, "NBS port")
	flag.StringVar(&cfg.NbsSocketsDir,
		"nbs-sockets-dir",
		"/run/nbsd/sockets",
		"Path to folder with disk sockets on the node",
	)
	flag.StringVar(&cfg.PodSocketsDir,
		"pod-sockets-dir",
		"/nbsd-sockets",
		"Path to folder with disk sockets on the pod",
	)
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
