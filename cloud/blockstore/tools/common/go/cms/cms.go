package cms

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"path"
	"reflect"
	"strings"

	"github.com/golang/protobuf/proto"
	"github.com/ydb-platform/nbs/cloud/blockstore/config"
	nbs "github.com/ydb-platform/nbs/cloud/blockstore/public/sdk/go/client"
	kikimr "github.com/ydb-platform/nbs/cloud/blockstore/tools/common/go/cms/proto"
	"github.com/ydb-platform/nbs/cloud/nbs_internal/storage/core/tools/common/go/pssh"
	logutil "github.com/ydb-platform/nbs/cloud/storage/core/tools/common/go/log"
)

////////////////////////////////////////////////////////////////////////////////

const (
	NBSConfigPrefix = "Cloud.NBS."
)

////////////////////////////////////////////////////////////////////////////////

type CMSClientIface interface {
	GetConfigs(ctx context.Context, target string) ([]proto.Message, error)
	UpdateConfig(
		ctx context.Context,
		target string,
		item proto.Message,
		cookie string,
	) error
	RemoveConfig(
		ctx context.Context,
		target string,
		configName string,
	) error
	AllowNamedConfigs(ctx context.Context) error
}

////////////////////////////////////////////////////////////////////////////////

func unmarshalConfig(data string, name string) (proto.Message, error) {
	if name == "DiskAgentConfig" {
		m := &config.TDiskAgentConfig{}
		err := proto.UnmarshalText(data, m)
		if err != nil {
			return nil, err
		}
		return m, nil
	}

	// TODO: more configs as needed

	return nil, fmt.Errorf("unknown config: %v", name)
}

////////////////////////////////////////////////////////////////////////////////

type cmsClient struct {
	logutil.WithLog

	host     string
	userName string
	tmpDir   string

	pssh pssh.PsshIface
}

func (client *cmsClient) dumpRequest(m proto.Message, prefix string) (string, error) {
	file, err := ioutil.TempFile(client.tmpDir, prefix)
	if err != nil {
		return "", fmt.Errorf("can't create temp file: %w", err)
	}

	defer file.Close()

	err = proto.MarshalText(file, m)
	if err != nil {
		return "", fmt.Errorf("can't save request to file: %w", err)
	}

	return file.Name(), nil
}

func (client *cmsClient) sendRequest(
	ctx context.Context,
	req *kikimr.TConsoleRequest,
	tmpFilePrefix string,
) (*kikimr.TConsoleResponse, error) {

	fileName, err := client.dumpRequest(req, tmpFilePrefix)
	if err != nil {
		return nil, err
	}

	err = client.pssh.CopyFile(
		ctx,
		fileName,
		fmt.Sprintf("%v:/home/%v/", client.host, client.userName),
	)
	if err != nil {
		return nil, fmt.Errorf("can't copy request to remote host: %w", err)
	}

	lines, err := client.pssh.RunOnHost(
		ctx,
		[]string{fmt.Sprintf("kikimr admin console execute %v", path.Base(fileName))},
		client.host,
	)
	if err != nil {
		return nil, fmt.Errorf("can't execute request: %w", err)
	}

	resp := &kikimr.TConsoleResponse{}
	err = proto.UnmarshalText(strings.Join(lines, "\n"), resp)
	if err != nil {
		return nil, fmt.Errorf("can't unmarshal response %v: %w", lines, err)
	}
	if resp.Status == nil {
		return nil, errors.New("empty response")
	}

	if *resp.Status.Code != kikimr.StatusIds_SUCCESS {
		return nil, fmt.Errorf(
			"response with error status: %v",
			resp.Status,
		)
	}

	if resp.Response == nil {
		return nil, errors.New("empty response")
	}

	return resp, nil
}

func (client *cmsClient) getConfigItems(
	ctx context.Context,
	target string,
) ([]*kikimr.TConfigItem, error) {

	var req = &kikimr.TConsoleRequest{
		Request: &kikimr.TConsoleRequest_GetConfigItemsRequest{
			GetConfigItemsRequest: &kikimr.TGetConfigItemsRequest{
				UsageScopes: []*kikimr.TUsageScope{
					&kikimr.TUsageScope{
						Filter: &kikimr.TUsageScope_HostFilter{
							HostFilter: &kikimr.THosts{
								Hosts: []string{
									target,
								},
							},
						},
					},
				},
				ItemKinds: []uint32{
					uint32(kikimr.TConfigItem_NamedConfigsItem),
				},
			},
		},
	}

	resp, err := client.sendRequest(ctx, req, "cms-get-items-")
	if err != nil {
		return nil, fmt.Errorf("GetConfigItemsRequest failed: %w", err)
	}

	r, ok := resp.Response.(*kikimr.TConsoleResponse_GetConfigItemsResponse)
	if !ok {
		return nil, fmt.Errorf("unexpected response type %T", r)
	}

	if r.GetConfigItemsResponse.Status == nil {
		return nil, errors.New("empty status for GetConfigItemsResponse")
	}

	if *r.GetConfigItemsResponse.Status.Code != kikimr.StatusIds_SUCCESS {
		return nil, fmt.Errorf(
			"GetConfigItemsRequest failed with status: %v",
			r.GetConfigItemsResponse.Status,
		)
	}

	if r.GetConfigItemsResponse.ConfigItems == nil {
		return nil, nil
	}

	var result []*kikimr.TConfigItem

	for _, item := range r.GetConfigItemsResponse.ConfigItems {
		if item.Config == nil || item.Kind == nil {
			continue
		}

		if *item.Kind != uint32(kikimr.TConfigItem_NamedConfigsItem) {
			continue
		}

		if len(item.Config.NamedConfigs) == 0 {
			continue
		}

		// TODO: assert len(item.Config.NamedConfigs) == 1
		if !strings.HasPrefix(*item.Config.NamedConfigs[0].Name, NBSConfigPrefix) {
			continue
		}

		result = append(result, item)
	}

	return result, nil
}

func (client *cmsClient) GetConfigs(
	ctx context.Context,
	target string,
) ([]proto.Message, error) {

	items, err := client.getConfigItems(ctx, target)
	if err != nil {
		return nil, err
	}

	var result []proto.Message

	for _, item := range items {
		for _, config := range item.Config.NamedConfigs {
			if !strings.HasPrefix(*config.Name, NBSConfigPrefix) {
				continue
			}

			configName := strings.TrimPrefix(*config.Name, NBSConfigPrefix)

			m, err := unmarshalConfig(string(config.Config), configName)
			if err != nil {
				client.LogWarn(ctx, "can't unmarshal config %v: %w", configName, err)
				continue
			}

			result = append(result, m)
		}
	}

	return result, nil
}

func (client *cmsClient) removeItems(
	ctx context.Context,
	ids ...*kikimr.TConfigItemId,
) error {

	if len(ids) == 0 {
		return nil
	}

	var actions []*kikimr.TConfigureAction
	for _, id := range ids {
		actions = append(
			actions,
			&kikimr.TConfigureAction{
				Action: &kikimr.TConfigureAction_RemoveConfigItem{
					RemoveConfigItem: &kikimr.TRemoveConfigItem{
						ConfigItemId: id,
					},
				},
			},
		)
	}

	var req = &kikimr.TConsoleRequest{
		Request: &kikimr.TConsoleRequest_ConfigureRequest{
			ConfigureRequest: &kikimr.TConfigureRequest{
				Actions: actions,
			},
		},
	}

	resp, err := client.sendRequest(ctx, req, "cms-remove-items-")
	if err != nil {
		return fmt.Errorf("ConfigureRequest failed: %w", err)
	}

	r, ok := resp.Response.(*kikimr.TConsoleResponse_ConfigureResponse)
	if !ok {
		return fmt.Errorf("unexpected response type %T", r)
	}

	if r.ConfigureResponse.Status == nil {
		return errors.New("empty status for ConfigureResponse")
	}

	if *r.ConfigureResponse.Status.Code != kikimr.StatusIds_SUCCESS {
		return fmt.Errorf(
			"ConfigureResponse failed with status: %v",
			r.ConfigureResponse.Status,
		)
	}

	return nil
}

func (client *cmsClient) removeOldConfigs(
	ctx context.Context,
	configName string,
	items ...*kikimr.TConfigItem,
) error {

	var toRemove []*kikimr.TConfigItemId
	for _, item := range items {
		name := *item.Config.NamedConfigs[0].Name

		if name == configName {
			toRemove = append(toRemove, item.Id)
		}
	}

	if len(toRemove) == 0 {
		return nil
	}

	err := client.removeItems(ctx, toRemove...)
	if err != nil {
		return fmt.Errorf("can't remove items: %w", err)
	}

	return nil
}

func newAddConfigItemAction(
	target string,
	configName string,
	configItem proto.Message,
	cookie string,
) *kikimr.TConfigureAction {

	kind := uint32(kikimr.TConfigItem_NamedConfigsItem)
	data := (&proto.TextMarshaler{Compact: true}).Text(configItem)
	mergeStrategy := uint32(kikimr.TConfigItem_MERGE)

	return &kikimr.TConfigureAction{
		Action: &kikimr.TConfigureAction_AddConfigItem{
			AddConfigItem: &kikimr.TAddConfigItem{
				ConfigItem: &kikimr.TConfigItem{
					Kind: &kind,
					Config: &kikimr.TAppConfig{
						NamedConfigs: []*kikimr.TNamedConfig{
							&kikimr.TNamedConfig{
								Name:   &configName,
								Config: []byte(data),
							},
						},
					},
					UsageScope: &kikimr.TUsageScope{
						Filter: &kikimr.TUsageScope_HostFilter{
							HostFilter: &kikimr.THosts{
								Hosts: []string{
									target,
								},
							},
						},
					},
					Order:         nil,
					MergeStrategy: &mergeStrategy,
					Cookie:        &cookie,
				},
			},
		},
	}
}

func (client *cmsClient) UpdateConfig(
	ctx context.Context,
	target string,
	newItem proto.Message,
	cookie string,
) error {

	itemName, err := MakeConfigItemName(newItem)
	if err != nil {
		return fmt.Errorf("can't make item name: %w", err)
	}

	items, err := client.getConfigItems(ctx, target)
	if err != nil {
		return fmt.Errorf("can't get items: %w", err)
	}

	var req = &kikimr.TConsoleRequest{
		Request: &kikimr.TConsoleRequest_ConfigureRequest{
			ConfigureRequest: &kikimr.TConfigureRequest{
				Actions: []*kikimr.TConfigureAction{
					newAddConfigItemAction(target, itemName, newItem, cookie),
				},
			},
		},
	}

	resp, err := client.sendRequest(ctx, req, "cms-update-item-")
	if err != nil {
		return fmt.Errorf("ConfigureRequest failed: %w", err)
	}

	r, ok := resp.Response.(*kikimr.TConsoleResponse_ConfigureResponse)
	if !ok {
		return fmt.Errorf("unexpected response type %T", r)
	}

	if r.ConfigureResponse.Status == nil {
		return errors.New("empty status for ConfigureResponse")
	}

	if *r.ConfigureResponse.Status.Code != kikimr.StatusIds_SUCCESS {
		return fmt.Errorf(
			"ConfigureResponse failed with status: %v",
			r.ConfigureResponse.Status,
		)
	}

	err = client.removeOldConfigs(ctx, itemName, items...)
	if err != nil {
		return fmt.Errorf("can't remove old items: %w", err)
	}

	return nil
}

func (client *cmsClient) AllowNamedConfigs(ctx context.Context) error {
	kinds := []uint32{
		uint32(kikimr.TConfigItem_NamedConfigsItem),
	}

	req := &kikimr.TConfig{
		ConfigsConfig: &kikimr.TConfigsConfig{
			UsageScopeRestrictions: &kikimr.TConfigsConfig_TUsageScopeRestrictions{
				AllowedHostUsageScopeKinds:     kinds,
				AllowedTenantUsageScopeKinds:   kinds,
				AllowedNodeTypeUsageScopeKinds: kinds,
			},
		},
	}

	fileName, err := client.dumpRequest(req, "cms-allow-")
	if err != nil {
		return err
	}

	err = client.pssh.CopyFile(
		ctx,
		fileName,
		fmt.Sprintf("%v:/home/%v/", client.host, client.userName),
	)
	if err != nil {
		return fmt.Errorf("can't copy request to remote host: %w", err)
	}

	lines, err := client.pssh.RunOnHost(
		ctx,
		[]string{fmt.Sprintf("kikimr admin console config set --merge %v", path.Base(fileName))},
		client.host,
	)
	if err != nil {
		return fmt.Errorf("can't execute request: %w", err)
	}

	if len(lines) != 1 || lines[0] != "OK" {
		return fmt.Errorf("unexpected output: %v", lines)
	}

	return nil
}

func (client *cmsClient) RemoveConfig(
	ctx context.Context,
	target string,
	configName string,
) error {

	items, err := client.getConfigItems(ctx, target)
	if err != nil {
		return fmt.Errorf("can't get items: %w", err)
	}

	err = client.removeOldConfigs(ctx, NBSConfigPrefix+configName, items...)
	if err != nil {
		return fmt.Errorf("can't remove items: %w", err)
	}

	return nil
}

////////////////////////////////////////////////////////////////////////////////

func NewClient(
	log nbs.Log,
	host string,
	userName string,
	tmpDir string,
	pssh pssh.PsshIface,
) CMSClientIface {

	return &cmsClient{
		WithLog: logutil.WithLog{
			Log: log,
		},
		host:     host,
		userName: userName,
		tmpDir:   tmpDir,
		pssh:     pssh,
	}
}

func MakeConfigItemName(item interface{}) (string, error) {
	var name string

	t := reflect.TypeOf(item)

	if t.Kind() == reflect.Ptr {
		name = t.Elem().Name()
	} else {
		name = t.Name()
	}

	return NBSConfigPrefix + name[1:], nil
}
