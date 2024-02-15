package main

import (
	"flag"

	"github.com/ydb-platform/nbs/cloud/blockstore/tools/csi_driver/internal/driver"
)

const driverName = "nbs.csi.nebius.ai"

var version = ""

func main() {
	if version == "" {
		version = "dev"
	}

	cfg := driver.Config{
		DriverName:    driverName,
		VendorVersion: version,
	}

	flag.StringVar(&cfg.Endpoint, "endpoint", "/csi/csi.sock", "CSI endpoint")
	flag.StringVar(&cfg.NodeID, "node-id", "", "Node id")

	flag.Parse()

	srv, err := driver.NewDriver(cfg)
	if err != nil {
		panic(err)
	}
	if err := srv.Run(cfg.Endpoint); err != nil {
		panic(err)
	}
}
