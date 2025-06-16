package admin

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/spf13/cobra"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	internal_client "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/client"
	client_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/configs/client/config"
	server_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/configs/server/config"
)

////////////////////////////////////////////////////////////////////////////////

type placementStrategy disk_manager.PlacementStrategy

var stringToPlacementStrategy = map[string]placementStrategy{
	"spread":    placementStrategy(disk_manager.PlacementStrategy_PLACEMENT_STRATEGY_SPREAD),
	"partition": placementStrategy(disk_manager.PlacementStrategy_PLACEMENT_STRATEGY_PARTITION),
}

func (s *placementStrategy) String() string {
	for name, val := range stringToPlacementStrategy {
		if val == *s {
			return name
		}
	}

	log.Fatalf("Unknown placement strategy: %v", *s)
	return ""
}

func (s *placementStrategy) Set(val string) error {
	var ok bool
	*s, ok = stringToPlacementStrategy[val]
	if !ok {
		return fmt.Errorf("unknown placement strategy %v", val)
	}

	return nil
}

func (s *placementStrategy) Type() string {
	return "PlacementStrategy"
}

////////////////////////////////////////////////////////////////////////////////

type getPlacementGroup struct {
	clientConfig     *client_config.ClientConfig
	serverConfig     *server_config.ServerConfig
	placementGroupID string
}

func (c *getPlacementGroup) run() error {
	ctx := newContext(c.clientConfig)

	resourceStorage, db, err := newResourceStorage(ctx, c.serverConfig)
	if err != nil {
		return err
	}
	defer db.Close(ctx)

	image, err := resourceStorage.GetPlacementGroupMeta(ctx, c.placementGroupID)
	if err != nil {
		return err
	}

	j, err := json.Marshal(image)
	if err != nil {
		return err
	}

	fmt.Println(string(j))

	return nil
}

func newGetPlacementGroupCmd(
	clientConfig *client_config.ClientConfig,
	serverConfig *server_config.ServerConfig,
) *cobra.Command {

	c := &getPlacementGroup{
		clientConfig: clientConfig,
		serverConfig: serverConfig,
	}

	cmd := &cobra.Command{
		Use: "get",
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.run()
		},
	}

	cmd.Flags().StringVar(
		&c.placementGroupID,
		"id",
		"",
		"placement group ID; required")
	if err := cmd.MarkFlagRequired("id"); err != nil {
		log.Fatalf("Error setting flag id as required: %v", err)
	}

	return cmd
}

////////////////////////////////////////////////////////////////////////////////

type listPlacementGroups struct {
	clientConfig *client_config.ClientConfig
	serverConfig *server_config.ServerConfig
	folderID     string
}

func (c *listPlacementGroups) run() error {
	ctx := newContext(c.clientConfig)

	resourceStorage, db, err := newResourceStorage(ctx, c.serverConfig)
	if err != nil {
		return err
	}
	defer db.Close(ctx)

	ids, err := resourceStorage.ListPlacementGroups(ctx, c.folderID, time.Now())
	if err != nil {
		return err
	}

	fmt.Println(strings.Join(ids, "\n"))

	return nil
}

func newListPlacementGroupsCmd(
	clientConfig *client_config.ClientConfig,
	serverConfig *server_config.ServerConfig,
) *cobra.Command {

	c := &listPlacementGroups{
		clientConfig: clientConfig,
		serverConfig: serverConfig,
	}

	cmd := &cobra.Command{
		Use: "list",
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.run()
		},
	}

	cmd.Flags().StringVar(
		&c.folderID,
		"folder-id",
		"",
		"ID of folder where placement groups are located; optional",
	)
	return cmd
}

////////////////////////////////////////////////////////////////////////////////

type createPlacementGroup struct {
	clientConfig            *client_config.ClientConfig
	zoneID                  string
	placementGroupID        string
	placementPartitionCount uint32
	placementStrategy       placementStrategy
}

func (c *createPlacementGroup) run() error {
	ctx := newContext(c.clientConfig)

	client, err := internal_client.NewClient(ctx, c.clientConfig)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}
	defer client.Close()

	req := &disk_manager.CreatePlacementGroupRequest{
		GroupId: &disk_manager.GroupId{
			ZoneId:  c.zoneID,
			GroupId: c.placementGroupID,
		},
		PlacementStrategy:       disk_manager.PlacementStrategy(c.placementStrategy),
		PlacementPartitionCount: c.placementPartitionCount,
	}

	resp, err := client.CreatePlacementGroup(getRequestContext(ctx), req)
	if err != nil {
		return err
	}

	fmt.Printf("Operation: %v\n", resp.Id)

	return internal_client.WaitOperation(ctx, client, resp.Id)
}

func newCreatePlacementGroupCmd(
	clientConfig *client_config.ClientConfig,
) *cobra.Command {

	c := &createPlacementGroup{
		clientConfig:      clientConfig,
		placementStrategy: placementStrategy(disk_manager.PlacementStrategy_PLACEMENT_STRATEGY_SPREAD),
	}

	cmd := &cobra.Command{
		Use: "create",
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.run()
		},
	}

	cmd.Flags().StringVar(
		&c.zoneID,
		"zone-id",
		"",
		"zone ID in which to create placementGroup; required",
	)
	if err := cmd.MarkFlagRequired("zone-id"); err != nil {
		log.Fatalf("Error setting flag zone-id as required: %v", err)
	}

	cmd.Flags().StringVar(
		&c.placementGroupID,
		"id",
		"",
		"placement group ID; required",
	)
	if err := cmd.MarkFlagRequired("id"); err != nil {
		log.Fatalf("Error setting flag id as required: %v", err)
	}

	cmd.Flags().Var(
		&c.placementStrategy,
		"placement-strategy",
		"placement strategy; optional; possible values [spread, partition]",
	)

	cmd.Flags().Uint32Var(
		&c.placementPartitionCount,
		"placement-partition-count",
		0,
		"partition count for 'partition' placement strategy",
	)

	return cmd
}

////////////////////////////////////////////////////////////////////////////////

type deletePlacementGroup struct {
	clientConfig     *client_config.ClientConfig
	zoneID           string
	placementGroupID string
}

func (c *deletePlacementGroup) run() error {
	ctx := newContext(c.clientConfig)

	err := requestConfirmation("placementGroup", c.placementGroupID)
	if err != nil {
		return err
	}

	client, err := internal_client.NewClient(ctx, c.clientConfig)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}
	defer client.Close()

	req := &disk_manager.DeletePlacementGroupRequest{
		GroupId: &disk_manager.GroupId{
			ZoneId:  c.zoneID,
			GroupId: c.placementGroupID,
		},
	}

	resp, err := client.DeletePlacementGroup(getRequestContext(ctx), req)
	if err != nil {
		return err
	}

	fmt.Printf("Operation: %v\n", resp.Id)

	return internal_client.WaitOperation(ctx, client, resp.Id)
}

func newDeletePlacementGroupCmd(
	clientConfig *client_config.ClientConfig,
) *cobra.Command {

	c := &deletePlacementGroup{
		clientConfig: clientConfig,
	}

	cmd := &cobra.Command{
		Use: "delete",
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.run()
		},
	}

	cmd.Flags().StringVar(
		&c.zoneID,
		"zone-id",
		"",
		"zone ID where placement group is located; required")
	if err := cmd.MarkFlagRequired("zone-id"); err != nil {
		log.Fatalf("Error setting flag zone-id as required: %v", err)
	}

	cmd.Flags().StringVar(
		&c.placementGroupID,
		"id",
		"",
		"placement group ID; required")
	if err := cmd.MarkFlagRequired("id"); err != nil {
		log.Fatalf("Error setting flag id as required: %v", err)
	}

	return cmd
}

////////////////////////////////////////////////////////////////////////////////

func newPlacementGroupCmd(
	clientConfig *client_config.ClientConfig,
	serverConfig *server_config.ServerConfig,
) *cobra.Command {

	cmd := &cobra.Command{
		Use:     "placement-groups",
		Aliases: []string{"placement-group", "placement_group", "placement_groups"},
	}

	cmd.AddCommand(
		newGetPlacementGroupCmd(clientConfig, serverConfig),
		newListPlacementGroupsCmd(clientConfig, serverConfig),
		newCreatePlacementGroupCmd(clientConfig),
		newDeletePlacementGroupCmd(clientConfig),
	)

	return cmd
}
