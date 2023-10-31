package client

import (
	"context"
	"log"
	"net"
	"os"
	"path"
	"reflect"
	"testing"

	nbs "github.com/ydb-platform/nbs/cloud/blockstore/public/sdk/go/client"
)

func TestDiscoveryClient(t *testing.T) {
	port := os.Getenv("LOCAL_KIKIMR_SECURE_NBS_SERVER_PORT")
	cert := path.Join(os.Getenv("TEST_CERT_FILES_DIR"), "server.crt")

	dummy, err := net.Listen("tcp", ":0")

	if err != nil {
		t.Errorf("unable to set up listener: %v", err)
		return
	}

	eps := []string{
		dummy.Addr().String(),
		"localhost:1",
		"localhost:" + port}

	cli, err := nbs.NewDiscoveryClient(
		eps,
		&nbs.GrpcClientOpts{Credentials: &nbs.ClientCredentials{RootCertsFile: cert}},
		&nbs.DurableClientOpts{},
		&nbs.DiscoveryClientOpts{},
		nbs.NewLog(log.New(os.Stderr, "", 0), nbs.LOG_DEBUG))

	if err != nil {
		t.Errorf("unable to create discovery client: %v", err)
		return
	}

	ctx := context.TODO()

	err = cli.CreateVolume(ctx, "vol0", 1000, &nbs.CreateVolumeOpts{BlockSize: 4096})

	if err != nil {
		t.Errorf("unable to create volume: %v", err)
		return
	}

	vol, err := cli.ListVolumes(ctx)

	if err != nil {
		t.Errorf("unable to list volumes: %v", err)
		return
	}

	vol0 := []string{"vol0"}

	if !reflect.DeepEqual(vol, vol0) {
		t.Errorf("unexpected volume list: %v != %v", vol, vol0)
	}
}
