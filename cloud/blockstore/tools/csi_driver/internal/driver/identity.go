package driver

import (
	"context"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

var pluginCapabilities = []*csi.PluginCapability{
	{
		Type: &csi.PluginCapability_Service_{
			Service: &csi.PluginCapability_Service{
				Type: csi.PluginCapability_Service_CONTROLLER_SERVICE,
			},
		},
	},
	{
		Type: &csi.PluginCapability_Service_{
			Service: &csi.PluginCapability_Service{
				Type: csi.PluginCapability_Service_VOLUME_ACCESSIBILITY_CONSTRAINTS,
			},
		},
	},
}

type identity struct {
	driverName, driverVersion string
	vmMode                    bool
	offlineResize             bool
}

func newIdentityService(driverName, driverVersion string, vmMode, offlineResize bool) csi.IdentityServer {
	return &identity{
		driverName:    driverName,
		driverVersion: driverVersion,
		vmMode:        vmMode,
		offlineResize: offlineResize,
	}
}

func (i *identity) GetPluginInfo(context.Context, *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	return &csi.GetPluginInfoResponse{
		Name:          i.driverName,
		VendorVersion: i.driverVersion,
	}, nil
}

func (i *identity) GetPluginCapabilities(context.Context, *csi.GetPluginCapabilitiesRequest) (*csi.GetPluginCapabilitiesResponse, error) {
	capabilities := append([]*csi.PluginCapability{}, pluginCapabilities...)
	if !i.vmMode {
		expansionType := csi.PluginCapability_VolumeExpansion_ONLINE
		if i.offlineResize {
			expansionType = csi.PluginCapability_VolumeExpansion_OFFLINE
		}
		capabilities = append(capabilities, &csi.PluginCapability{
			Type: &csi.PluginCapability_VolumeExpansion_{
				VolumeExpansion: &csi.PluginCapability_VolumeExpansion{Type: expansionType},
			},
		})
	}
	return &csi.GetPluginCapabilitiesResponse{Capabilities: capabilities}, nil
}

func (*identity) Probe(context.Context, *csi.ProbeRequest) (*csi.ProbeResponse, error) {
	return &csi.ProbeResponse{
		Ready: &wrapperspb.BoolValue{
			Value: true,
		},
	}, nil
}
