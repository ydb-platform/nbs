package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

////////////////////////////////////////////////////////////////////////////////

type BackendValue string

func (b *BackendValue) String() string {
	return string(*b)
}

func (b *BackendValue) Set(val string) error {
	switch val {
	case "nbs", "nfs":
		*b = BackendValue(val)
		return nil
	default:
		return errors.New(`must be one of "nbs", "nfs"`)
	}
}

func (b *BackendValue) Type() string {
	return "string"
}

////////////////////////////////////////////////////////////////////////////////

func dialGrpcContext(
	ctx context.Context,
	endpoint string,
) (*grpc.ClientConn, error) {
	conn, err := grpc.DialContext(
		ctx,
		endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(
			func(ctx context.Context, address string) (net.Conn, error) {
				return net.Dial("unix", address)
			},
		),
	)
	if err != nil {
		return nil, err
	}

	return conn, err
}

func newNodeClient(
	ctx context.Context,
	endpoint string,
) (csi.NodeClient, error) {
	conn, err := dialGrpcContext(ctx, endpoint)
	if err != nil {
		return nil, err
	}

	return csi.NewNodeClient(conn), nil
}

func newControllerClient(
	ctx context.Context,
	endpoint string,
) (csi.ControllerClient, error) {
	conn, err := dialGrpcContext(ctx, endpoint)
	if err != nil {
		return nil, err
	}

	return csi.NewControllerClient(conn), nil
}

////////////////////////////////////////////////////////////////////////////////

func newCreateVolumeCommand(endpoint *string) *cobra.Command {
	var name string
	var size int64
	backend := BackendValue("nbs")
	cmd := cobra.Command{
		Use:   "createvolume",
		Short: "Send create volume request to the controller",
		Run: func(cmd *cobra.Command, args []string) {
			ctx, cancelFunc := context.WithTimeout(
				context.Background(),
				120*time.Second,
			)
			defer cancelFunc()
			client, err := newControllerClient(ctx, *endpoint)
			if err != nil {
				log.Fatal(err)
			}

			singleNodeCap := csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER
			capabilities := []*csi.VolumeCapability{
				{
					AccessType: &csi.VolumeCapability_Mount{
						Mount: nil,
					},
					AccessMode: &csi.VolumeCapability_AccessMode{
						Mode: singleNodeCap,
					},
				},
			}
			parameters := map[string]string{
				"backend": string(backend),
			}

			response, err := client.CreateVolume(
				ctx,
				&csi.CreateVolumeRequest{
					Name: name,
					CapacityRange: &csi.CapacityRange{
						RequiredBytes: size,
						LimitBytes:    0,
					},
					VolumeCapabilities: capabilities,
					AccessibilityRequirements: &csi.TopologyRequirement{
						Requisite: []*csi.Topology{
							{
								Segments: map[string]string{
									"topology.nbs.csi/node": "minikube",
								},
							},
						},
						Preferred: []*csi.Topology{
							{
								Segments: map[string]string{
									"topology.nbs.csi/node": "minikube",
								},
							},
						},
					},
					Parameters: parameters,
				},
			)

			if err != nil {
				log.Fatal(err)
			}

			log.Printf("Response: %v", response)
		},
	}
	cmd.Flags().StringVar(
		&name,
		"name",
		"",
		"The suggested name for the storage space.",
	)
	cmd.Flags().Int64Var(
		&size,
		"size",
		0,
		"The size of the disk in bytes",
	)
	cmd.Flags().Var(
		&backend,
		"backend",
		"Specify backend to use [nfs, nbs]",
	)
	err := cmd.MarkFlagRequired("name")
	if err != nil {
		log.Fatal(err)
	}

	err = cmd.MarkFlagRequired("size")
	if err != nil {
		log.Fatal(err)
	}

	return &cmd
}

func newDeleteVolumeCommand(endpoint *string) *cobra.Command {
	var volumeId string
	cmd := cobra.Command{
		Use:   "deletevolume",
		Short: "Send delete volume request to the controller",
		Run: func(cmd *cobra.Command, args []string) {
			ctx, cancelFunc := context.WithTimeout(
				context.Background(),
				120*time.Second,
			)
			defer cancelFunc()
			client, err := newControllerClient(ctx, *endpoint)
			if err != nil {
				log.Fatal(err)
			}

			response, err := client.DeleteVolume(ctx, &csi.DeleteVolumeRequest{
				VolumeId: volumeId,
				Secrets:  nil,
			})

			if err != nil {
				log.Fatal(err)
			}

			log.Printf("Response: %v", response)
		},
	}
	cmd.Flags().StringVar(&volumeId, "id", "", "volume id")
	err := cmd.MarkFlagRequired("id")
	if err != nil {
		log.Fatal(err)
	}

	return &cmd
}

func createVolumeCapability(
	accessMode csi.VolumeCapability_AccessMode_Mode,
	accessType string,
	volumeMountGroup string) *csi.VolumeCapability {
	volumeCapability := &csi.VolumeCapability{
		AccessMode: &csi.VolumeCapability_AccessMode{Mode: accessMode},
		AccessType: &csi.VolumeCapability_Mount{
			Mount: &csi.VolumeCapability_MountVolume{
				VolumeMountGroup: volumeMountGroup,
			},
		},
	}
	if accessType == "block" {
		volumeCapability.AccessType = &csi.VolumeCapability_Block{
			Block: &csi.VolumeCapability_BlockVolume{},
		}
	}

	return volumeCapability
}

func getTargetPath(podId string, volumeId string, accessType string) string {
	targetPathPattern := "/var/lib/kubelet/pods/%s/volumes/kubernetes.io~csi/%s/mount"
	if accessType == "block" {
		targetPathPattern = "/var/lib/kubelet/plugins/kubernetes.io/csi/volumeDevices/publish/%s/%s"
	}
	targetPath := fmt.Sprintf(
		targetPathPattern,
		podId,
		volumeId,
	)
	return targetPath
}

func newNodeStageVolumeCommand(endpoint *string) *cobra.Command {
	var volumeId, stagingTargetPath, accessType, vhostRequestQueuesCount string
	backend := BackendValue("nbs")
	cmd := cobra.Command{
		Use:   "stagevolume",
		Short: "Send stage volume request to the CSI node",
		Run: func(cmd *cobra.Command, args []string) {
			ctx, cancelFunc := context.WithTimeout(
				context.Background(),
				120*time.Second,
			)
			defer cancelFunc()
			client, err := newNodeClient(ctx, *endpoint)
			if err != nil {
				log.Fatal(err)
			}

			volumeContext := map[string]string{
				"instanceId":         "example-instance-id",
				"backend":            string(backend),
				"requestQueuesCount": vhostRequestQueuesCount,
			}

			accessMode := csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER
			response, err := client.NodeStageVolume(
				ctx,
				&csi.NodeStageVolumeRequest{
					VolumeId:          volumeId,
					StagingTargetPath: stagingTargetPath,
					VolumeCapability:  createVolumeCapability(accessMode, accessType, ""),
					VolumeContext:     volumeContext,
				},
			)
			if err != nil {
				log.Fatal(err)
			}

			log.Printf("Response: %v", response)
		},
	}
	cmd.Flags().StringVar(
		&volumeId,
		"volume-id",
		"",
		"volume id",
	)
	cmd.Flags().StringVar(
		&stagingTargetPath,
		"staging-target-path",
		"/var/lib/kubelet/plugins/kubernetes.io/csi/nbs.csi.nebius.ai/"+
			"a/globalmount",
		"staging target path",
	)
	cmd.Flags().StringVar(
		&accessType,
		"access-type",
		"mount",
		"mount or block access type",
	)
	cmd.Flags().Var(
		&backend,
		"backend",
		"Specify backend to use [nfs, nbs]",
	)
	cmd.Flags().StringVar(
		&vhostRequestQueuesCount,
		"vhost-request-queues-count",
		"8",
		"Specify vhost request queues count",
	)

	err := cmd.MarkFlagRequired("volume-id")
	if err != nil {
		log.Fatal(err)
	}
	return &cmd
}

func newPublishVolumeCommand(endpoint *string) *cobra.Command {
	var volumeId, podId, stagingTargetPath, podName, fsType string
	var accessType string
	var readOnly bool
	var volumeMountGroup string
	backend := BackendValue("nbs")
	cmd := cobra.Command{
		Use:   "publishvolume",
		Short: "Send publish volume request to the CSI node",
		Run: func(cmd *cobra.Command, args []string) {
			ctx, cancelFunc := context.WithTimeout(
				context.Background(),
				120*time.Second,
			)
			defer cancelFunc()
			client, err := newNodeClient(ctx, *endpoint)
			if err != nil {
				log.Fatal(err)
			}

			volumeContext := map[string]string{
				"csi.storage.k8s.io/pod.uid":                   podId,
				"csi.storage.k8s.io/serviceAccount.name":       "default",
				"csi.storage.k8s.io/ephemeral":                 "false",
				"csi.storage.k8s.io/pod.namespace":             "default",
				"csi.storage.k8s.io/pod.name":                  podName,
				"storage.kubernetes.io/csiProvisionerIdentity": "someIdentity",
				"instanceId": "example-instance-id",
				"fsType":     fsType,
				"backend":    string(backend),
			}

			accessMode := csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER

			response, err := client.NodePublishVolume(
				ctx,
				&csi.NodePublishVolumeRequest{
					VolumeId:          volumeId,
					PublishContext:    nil,
					StagingTargetPath: stagingTargetPath,
					TargetPath:        getTargetPath(podId, volumeId, accessType),
					VolumeCapability:  createVolumeCapability(accessMode, accessType, volumeMountGroup),
					Readonly:          readOnly,
					Secrets:           nil,
					VolumeContext:     volumeContext,
				},
			)
			if err != nil {
				log.Fatal(err)
			}

			log.Printf("Response: %v", response)
		},
	}
	cmd.Flags().StringVar(
		&volumeId,
		"volume-id",
		"",
		"volume id",
	)
	cmd.Flags().StringVar(&podId, "pod-id", "", "pod id")
	cmd.Flags().StringVar(
		&stagingTargetPath,
		"staging-target-path",
		"/var/lib/kubelet/plugins/kubernetes.io/csi/nbs.csi.nebius.ai/"+
			"a/globalmount",
		"staging target path",
	)
	cmd.Flags().StringVar(
		&podName,
		"pod-name",
		"some-pod",
		"pod name",
	)
	cmd.Flags().BoolVar(
		&readOnly,
		"readonly",
		false,
		"volume is read only",
	)
	cmd.Flags().StringVar(
		&fsType,
		"fs-type",
		"",
		"filesystem type: ext4, xfs",
	)
	cmd.Flags().StringVar(
		&accessType,
		"access-type",
		"mount",
		"mount or block access type",
	)
	cmd.Flags().StringVar(
		&volumeMountGroup,
		"volume-mount-group",
		"",
		"fs group id",
	)
	cmd.Flags().Var(
		&backend,
		"backend",
		"Specify backend to use [nfs, nbs]",
	)

	err := cmd.MarkFlagRequired("volume-id")
	if err != nil {
		log.Fatal(err)
	}

	err = cmd.MarkFlagRequired("pod-id")
	if err != nil {
		log.Fatal(err)
	}
	return &cmd
}

func newNodeUnstageVolumeCommand(endpoint *string) *cobra.Command {
	var volumeId, stagingTargetPath string
	cmd := cobra.Command{
		Use:   "unstagevolume",
		Short: "Send unstage volume request to the CSI node",
		Run: func(cmd *cobra.Command, args []string) {
			ctx, cancelFunc := context.WithTimeout(
				context.Background(),
				120*time.Second,
			)
			defer cancelFunc()
			client, err := newNodeClient(ctx, *endpoint)
			if err != nil {
				log.Fatal(err)
			}

			response, err := client.NodeUnstageVolume(
				ctx,
				&csi.NodeUnstageVolumeRequest{
					VolumeId:          volumeId,
					StagingTargetPath: stagingTargetPath,
				},
			)
			if err != nil {
				log.Fatal(err)
			}

			log.Printf("Response: %v", response)
		},
	}
	cmd.Flags().StringVar(
		&volumeId,
		"volume-id",
		"",
		"volume id",
	)
	cmd.Flags().StringVar(
		&stagingTargetPath,
		"staging-target-path",
		"/var/lib/kubelet/plugins/kubernetes.io/csi/nbs.csi.nebius.ai/"+
			"a/globalmount",
		"staging target path",
	)
	err := cmd.MarkFlagRequired("volume-id")
	if err != nil {
		log.Fatal(err)
	}

	return &cmd
}

func newUnpublishVolumeCommand(endpoint *string) *cobra.Command {
	var volumeId, podId, accessType string
	cmd := cobra.Command{
		Use:   "unpublishvolume",
		Short: "Send unpublish volume request to the CSI node",
		Run: func(cmd *cobra.Command, args []string) {
			ctx, cancelFunc := context.WithTimeout(
				context.Background(),
				120*time.Second,
			)
			defer cancelFunc()
			client, err := newNodeClient(ctx, *endpoint)

			response, err := client.NodeUnpublishVolume(
				ctx,
				&csi.NodeUnpublishVolumeRequest{
					VolumeId:   volumeId,
					TargetPath: getTargetPath(podId, volumeId, accessType),
				},
			)
			if err != nil {
				log.Fatal(err)
			}

			log.Printf("Response: %v", response)
		},
	}
	cmd.Flags().StringVar(
		&volumeId,
		"volume-id",
		"",
		"volume id",
	)
	cmd.Flags().StringVar(&podId, "pod-id", "", "pod id")
	cmd.Flags().StringVar(
		&accessType,
		"access-type",
		"mount",
		"mount or block access type",
	)
	err := cmd.MarkFlagRequired("volume-id")
	if err != nil {
		log.Fatal(err)
	}

	err = cmd.MarkFlagRequired("pod-id")
	if err != nil {
		log.Fatal(err)
	}
	return &cmd
}

////////////////////////////////////////////////////////////////////////////////

func newNodeGetVolumeStatsCommand(endpoint *string) *cobra.Command {
	var volumeId, podId string
	cmd := cobra.Command{
		Use:   "volumestats",
		Short: "get volume stats",
		Run: func(cmd *cobra.Command, args []string) {
			ctx, cancelFunc := context.WithTimeout(
				context.Background(),
				120*time.Second,
			)
			defer cancelFunc()
			client, err := newNodeClient(ctx, *endpoint)

			volumePath := fmt.Sprintf(
				"/var/lib/kubelet/pods/%s/volumes/kubernetes.io~csi/"+
					"%s/mount",
				podId,
				volumeId,
			)
			response, err := client.NodeGetVolumeStats(
				ctx,
				&csi.NodeGetVolumeStatsRequest{
					VolumeId:   volumeId,
					VolumePath: volumePath,
				},
			)
			if err != nil {
				log.Fatal(err)
			}

			jsonBytes, _ := json.MarshalIndent(response, "", "    ")
			fmt.Println(string(jsonBytes))
		},
	}
	cmd.Flags().StringVar(
		&volumeId,
		"volume-id",
		"",
		"volume id",
	)
	cmd.Flags().StringVar(&podId, "pod-id", "", "pod id")
	err := cmd.MarkFlagRequired("volume-id")
	if err != nil {
		log.Fatal(err)
	}

	err = cmd.MarkFlagRequired("pod-id")
	if err != nil {
		log.Fatal(err)
	}
	return &cmd
}

////////////////////////////////////////////////////////////////////////////////

func newNodeExpandVolumeCommand(endpoint *string) *cobra.Command {
	var volumeId, podId, accessType, stagingTargetPath string
	var size int64
	cmd := cobra.Command{
		Use:   "expandvolume",
		Short: "expand volume",
		Run: func(cmd *cobra.Command, args []string) {
			ctx, cancelFunc := context.WithTimeout(
				context.Background(),
				120*time.Second,
			)
			defer cancelFunc()
			client, err := newNodeClient(ctx, *endpoint)
			accessMode := csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER
			_, err = client.NodeExpandVolume(
				ctx,
				&csi.NodeExpandVolumeRequest{
					VolumeId:          volumeId,
					VolumePath:        getTargetPath(podId, volumeId, accessType),
					StagingTargetPath: stagingTargetPath,
					CapacityRange: &csi.CapacityRange{
						RequiredBytes: size,
					},
					VolumeCapability: createVolumeCapability(accessMode, accessType, ""),
				},
			)
			if err != nil {
				log.Fatal(err)
			}
		},
	}
	cmd.Flags().StringVar(
		&volumeId,
		"volume-id",
		"",
		"volume id",
	)
	cmd.Flags().StringVar(&podId, "pod-id", "", "pod id")
	cmd.Flags().StringVar(
		&stagingTargetPath,
		"staging-target-path",
		"/var/lib/kubelet/plugins/kubernetes.io/csi/nbs.csi.nebius.ai/"+
			"a/globalmount",
		"staging target path",
	)
	cmd.Flags().Int64Var(
		&size,
		"size",
		0,
		"The new size of the disk in bytes")
	cmd.Flags().StringVar(
		&accessType,
		"access-type",
		"mount",
		"mount or block access type",
	)

	err := cmd.MarkFlagRequired("volume-id")
	if err != nil {
		log.Fatal(err)
	}

	err = cmd.MarkFlagRequired("pod-id")
	if err != nil {
		log.Fatal(err)
	}

	err = cmd.MarkFlagRequired("size")
	if err != nil {
		log.Fatal(err)
	}
	return &cmd
}

////////////////////////////////////////////////////////////////////////////////

func newCsiNodeCommand(endpoint *string) *cobra.Command {
	cmd := cobra.Command{
		Use:   "node",
		Short: "CSI Driver node commands",
	}
	cmd.AddCommand(
		newPublishVolumeCommand(endpoint),
		newUnpublishVolumeCommand(endpoint),
		newNodeGetVolumeStatsCommand(endpoint),
		newNodeExpandVolumeCommand(endpoint),
		newNodeStageVolumeCommand(endpoint),
		newNodeUnstageVolumeCommand(endpoint),
	)
	return &cmd
}

func newCsiControllerCommand(endpoint *string) *cobra.Command {
	cmd := cobra.Command{
		Use:   "controller",
		Short: "CSI Driver controller command",
	}
	cmd.AddCommand(
		newCreateVolumeCommand(endpoint),
		newDeleteVolumeCommand(endpoint),
	)
	return &cmd
}

////////////////////////////////////////////////////////////////////////////////

func main() {
	var grpcEndpoint string
	rootCmd := &cobra.Command{
		Use:   "csi-client",
		Short: "CSI driver console client for debug",
	}
	rootCmd.PersistentFlags().StringVar(
		&grpcEndpoint,
		"endpoint",
		"csi.sock",
		"Path to the client config file",
	)

	rootCmd.AddCommand(
		newCsiNodeCommand(&grpcEndpoint),
		newCsiControllerCommand(&grpcEndpoint),
	)
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
