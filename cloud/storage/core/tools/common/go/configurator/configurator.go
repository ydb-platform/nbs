package configurator

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"text/template"

	"github.com/golang/protobuf/proto"
	nbs "github.com/ydb-platform/nbs/cloud/blockstore/public/sdk/go/client"
	logutil "github.com/ydb-platform/nbs/cloud/storage/core/tools/common/go/log"
	"golang.org/x/exp/maps"
)

////////////////////////////////////////////////////////////////////////////////

type ConfigMap = map[string]ConfigDescription

type ConfigDescription struct {
	Proto    proto.Message
	FileName string
}

////////////////////////////////////////////////////////////////////////////////

type CfgFileOverride struct {
	Domain          string            `yaml:"domain"`
	MonAddress      string            `yaml:"monAddress"`
	MonPort         string            `yaml:"monPort"`
	IcPort          string            `yaml:"icPort"`
	IcDiskAgentPort string            `yaml:"icDiskAgentPort"`
	VHostIcPort     string            `yaml:"vHostIcPort"`
	CustomOverrides map[string]string `yaml:"customOverrides"`
	LookupFunc      template.FuncMap
}

type ClusterSpec struct {
	Targets                         []string                   `yaml:"targets"`
	Zones                           []string                   `yaml:"zones"`
	ZoneCfgOverride                 map[string]CfgFileOverride `yaml:"zoneCfgOverride"`
	ZoneCfgOverrideSeed             map[string]CfgFileOverride `yaml:"zoneCfgOverrideSeed"`
	AdditionalFiles                 []string                   `yaml:"additionalFiles"`
	AdditionalFilesPath             string                     `yaml:"additionalFilesPath"`
	AdditionalFilesPathTargetPrefix string                     `yaml:"additionalFilesPathTargetPrefix"`
	TargetForSeed                   string                     `yaml:"targetForSeed"`
	GenerateSeed                    bool                       `yaml:"generateSeed"`
	Configs                         ConfigsSpec                `yaml:"configs"`
	Values                          ValuesSpec                 `yaml:"values"`
}

type Clusters map[string]ClusterSpec

type ServiceSpec struct {
	ServiceName     string              `yaml:"name"`
	CfgFileNames    map[string][]string `yaml:"cfgFileNames"`
	Clusters        Clusters            `yaml:"clusters"`
	DefaultOverride CfgFileOverride     `yaml:"defaultCfgOverride"`
}

type ConfigsSpec struct {
	Generate         bool   `yaml:"generate"`
	DumpPath         string `yaml:"dumpPath"`
	PathTargetPrefix string `yaml:"pathTargetPrefix"`
}

type ValuesSpec struct {
	Generate bool   `yaml:"generate"`
	FileName string `yaml:"fileName"`
	DumpPath string `yaml:"dumpPath"`
}

type GeneratorSpec struct {
	ServiceSpec   ServiceSpec
	ConfigMap     ConfigMap
	ArcadiaPath   string
	OverridesPath string
}

type ConfigGenerator struct {
	logutil.WithLog

	spec GeneratorSpec
}

////////////////////////////////////////////////////////////////////////////////

type ResultConfig struct {
	FileName string
	Text     string
}

////////////////////////////////////////////////////////////////////////////////

func (g *ConfigGenerator) updateConfigMapFromDir(
	ctx context.Context,
	overridesPath string,
	configProto *ConfigMap,
	cfgOverride CfgFileOverride,
) error {

	for resultConfigFileName, configDesc := range *configProto {
		configProtoPath := path.Join(overridesPath, configDesc.FileName)
		g.LogDbg(ctx, "Reading file %v", configProtoPath)

		configData, err := ioutil.ReadFile(configProtoPath)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf(
				"failed to read protobuf from config proto file %v: %w",
				configProtoPath,
				err,
			)
		}

		configOverrided, err := g.applyOverrides(
			ctx,
			configProtoPath,
			configData,
			cfgOverride)
		if err != nil {
			return err
		}

		newConfigProto := proto.Clone(configDesc.Proto)
		err = proto.UnmarshalText(configOverrided, newConfigProto)
		if err != nil {
			return fmt.Errorf(
				"failed to parse protobuf from config proto file %v: %w",
				configProtoPath,
				err,
			)
		}

		proto.Merge(configDesc.Proto, newConfigProto)
		(*configProto)[resultConfigFileName] = configDesc
	}
	return nil
}

func (g *ConfigGenerator) loadOverrides(
	ctx context.Context,
	target string,
	overridesPath string,
	configMap *ConfigMap,
	cfgOverride CfgFileOverride,
) error {

	err := g.updateConfigMapFromDir(ctx, overridesPath, configMap, cfgOverride)
	if err != nil {
		return err
	}

	return g.updateConfigMapFromDir(
		ctx,
		path.Join(overridesPath, target),
		configMap,
		cfgOverride)
}

func (g *ConfigGenerator) loadAllOverrides(
	ctx context.Context,
	target string,
	cluster string,
	zone string,
	configMap *ConfigMap,
	seed bool) error {

	g.LogDbg(
		ctx,
		"Reading overrides for cluster %v, zone %v, target %v",
		cluster,
		zone,
		target)

	cfgOverride := g.constructCfgOverride(ctx, cluster, zone, seed)
	err := g.loadOverrides(
		ctx,
		target,
		path.Join(g.spec.OverridesPath, "common"),
		configMap,
		cfgOverride)
	if err != nil {
		return err
	}

	err = g.loadOverrides(
		ctx,
		target,
		path.Join(g.spec.OverridesPath, cluster, "common"),
		configMap,
		cfgOverride)
	if err != nil {
		return err
	}

	err = g.loadOverrides(
		ctx,
		target,
		path.Join(g.spec.OverridesPath, cluster, zone),
		configMap,
		cfgOverride)
	if err != nil {
		return err
	}

	if seed {
		return g.loadOverrides(
			ctx,
			target,
			path.Join(g.spec.OverridesPath, "common", "seed", zone),
			configMap,
			cfgOverride)
	}

	return nil
}

func (g *ConfigGenerator) applyOverrides(
	ctx context.Context,
	templateName string,
	tempateBody []byte,
	override CfgFileOverride,
) (string, error) {
	tmpl, err := template.New(templateName).Funcs(override.LookupFunc).Parse(string(tempateBody))
	if err != nil {
		return "", fmt.Errorf(
			"failed to parse cfg file %v: %w",
			tempateBody,
			err,
		)
	}

	var result bytes.Buffer
	err = tmpl.Execute(&result, override)
	if err != nil {
		return "", fmt.Errorf(
			"failed to generate cfg file %v: %w",
			templateName,
			err,
		)
	}
	return result.String(), nil
}

func (g *ConfigGenerator) dumpConfigs(
	ctx context.Context,
	configs *ConfigMap,
	target string,
	cluster string,
	zone string,
	seed bool,
) error {

	keys := maps.Keys(*configs)
	sort.Strings(keys)

	var resultConfigs []ResultConfig
	for _, resultConfigName := range keys {
		configDesc := (*configs)[resultConfigName]
		if proto.Size(configDesc.Proto) == 0 {
			continue
		}
		str := proto.MarshalTextString(configDesc.Proto)

		// replaces are needed for dump in format like kikimr-configure does
		str = strings.ReplaceAll(str, "<", "{")
		str = strings.ReplaceAll(str, ">", "}")
		str = strings.ReplaceAll(str, ": {", " {")
		resultConfigs = append(resultConfigs, ResultConfig{resultConfigName, str})
	}

	for _, cfgFile := range g.spec.ServiceSpec.CfgFileNames[target] {
		cfgFilePath := path.Join(
			g.spec.OverridesPath,
			cfgFile)
		if _, err := os.Stat(path.Join(g.spec.OverridesPath, cluster, cfgFile)); err == nil {
			cfgFilePath = path.Join(g.spec.OverridesPath, cluster, cfgFile)
		}
		cfgFileData, err := ioutil.ReadFile(cfgFilePath)
		if err != nil {
			return fmt.Errorf(
				"failed to read cfg file %v: %w",
				cfgFile,
				err,
			)
		}

		cfgOverride := g.constructCfgOverride(ctx, cluster, zone, seed)
		configOverrided, err := g.applyOverrides(
			ctx,
			cfgFile,
			cfgFileData,
			cfgOverride,
		)
		if err != nil {
			return err
		}

		resultConfig := ResultConfig{cfgFile, configOverrided}
		resultConfigs = append(resultConfigs, resultConfig)
	}

	for _, fileName := range g.spec.ServiceSpec.Clusters[cluster].AdditionalFiles {
		var filePath string
		if strings.HasPrefix(fileName, "/") {
			filePath = path.Join(g.spec.ArcadiaPath, fileName)
		} else {
			filePath = path.Join(
				g.spec.ArcadiaPath,
				g.spec.ServiceSpec.Clusters[cluster].AdditionalFilesPath,
				zone,
				target,
				g.spec.ServiceSpec.Clusters[cluster].AdditionalFilesPathTargetPrefix,
				fileName)
		}
		fileData, err := ioutil.ReadFile(filePath)
		if err != nil {
			return fmt.Errorf(
				"failed to read file %v: %w",
				filePath,
				err,
			)
		}
		resultConfigs = append(
			resultConfigs,
			ResultConfig{filepath.Base(fileName), string(fileData)})
	}

	if g.spec.ServiceSpec.Clusters[cluster].Configs.Generate && !seed {
		err := g.dumpTxtConfigs(ctx, resultConfigs, path.Join(
			g.spec.ArcadiaPath,
			g.spec.ServiceSpec.Clusters[cluster].Configs.DumpPath,
			zone,
			target,
			g.spec.ServiceSpec.Clusters[cluster].Configs.PathTargetPrefix))
		if err != nil {
			return fmt.Errorf(
				"failed to generate txt configs: %w",
				err,
			)
		}
	}

	if g.spec.ServiceSpec.Clusters[cluster].Values.Generate {
		valuesTemplatePath := path.Join(g.spec.OverridesPath, "values.yaml")
		configPath := path.Join(
			g.spec.ArcadiaPath,
			g.spec.ServiceSpec.Clusters[cluster].Values.DumpPath,
			cluster,
			zone)
		targetName := target
		if seed {
			targetName = "seed"
		}
		err := g.dumpValues(
			ctx,
			resultConfigs,
			valuesTemplatePath,
			path.Join(configPath, targetName),
			g.spec.ServiceSpec.Clusters[cluster].Values.FileName)
		if err != nil {
			return fmt.Errorf(
				"failed to generate values for target '%v': %w",
				target,
				err,
			)
		}
	}

	return nil
}

func (g *ConfigGenerator) dumpTxtConfigs(
	ctx context.Context,
	configList []ResultConfig,
	configPath string,
) error {

	g.LogDbg(ctx, "dump configs to %v", configPath)

	err := os.MkdirAll(configPath, 0755)
	if err != nil {
		return fmt.Errorf(
			"cannot create directories %v for configs: %w",
			configPath,
			err,
		)
	}
	for _, cfg := range configList {
		g.LogDbg(ctx, "dump config %v", path.Join(configPath, cfg.FileName))
		file, err := os.OpenFile(
			path.Join(configPath, cfg.FileName),
			os.O_WRONLY|os.O_CREATE|os.O_TRUNC,
			0755)
		if err != nil {
			return fmt.Errorf(
				"cannot open or create file %v: %w",
				file,
				err,
			)
		}
		result := cfg.Text
		if !strings.HasSuffix(result, "\n") {
			result += "\n"
		}
		_, err = file.WriteString(result)
		if err != nil {
			return fmt.Errorf(
				"cannot write to file %v: %w",
				file,
				err,
			)
		}
		err = file.Close()
		if err != nil {
			return fmt.Errorf(
				"cannot close file %v: %w",
				file,
				err,
			)
		}
	}
	return nil
}

func trimWhiteSpaceLines(filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	tmpFile, err := os.OpenFile(
		filePath+".tmp",
		os.O_WRONLY|os.O_CREATE|os.O_TRUNC,
		0755)
	if err != nil {
		return err
	}
	defer tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	whiteSpaceLineRe := regexp.MustCompile(`^\s*$`)

	scanner := bufio.NewScanner(file)
	writer := bufio.NewWriter(tmpFile)
	for scanner.Scan() {
		line := scanner.Text()
		if !whiteSpaceLineRe.MatchString(line) {
			_, err = writer.WriteString(line + "\n")
		} else {
			_, err = writer.WriteString("\n")
		}
		if err != nil {
			return err
		}
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	err = writer.Flush()
	if err != nil {
		return err
	}
	err = tmpFile.Close()
	if err != nil {
		return err
	}

	return os.Rename(tmpFile.Name(), filePath)
}

func (g *ConfigGenerator) dumpValues(
	ctx context.Context,
	configs []ResultConfig,
	valuesTemplatePath string,
	dumpPath string,
	fileName string,
) error {
	g.LogDbg(ctx, "dump values to %v", dumpPath)

	var resultConfigs []ResultConfig
	for _, cfg := range configs {
		if len(cfg.Text) == 0 {
			continue
		}
		resultConfigs = append(
			resultConfigs,
			ResultConfig{
				cfg.FileName,
				strings.ReplaceAll(cfg.Text, "\n", "\n    ")})
	}

	valuesTemplate, err := template.ParseFiles(valuesTemplatePath)
	if err != nil {
		return fmt.Errorf(
			"cannot read text template from file: %w",
			err,
		)
	}
	err = os.MkdirAll(dumpPath, 0755)
	if err != nil {
		return fmt.Errorf(
			"cannot create directories: %w",
			err,
		)
	}

	filePath := path.Join(dumpPath, fileName)
	file, err := os.OpenFile(
		filePath,
		os.O_WRONLY|os.O_CREATE|os.O_TRUNC,
		0755)
	if err != nil {
		return fmt.Errorf(
			"cannot open or create file %v: %w",
			file,
			err,
		)
	}
	err = valuesTemplate.Execute(file, struct {
		Configs []ResultConfig
	}{
		resultConfigs})
	if err != nil {
		return fmt.Errorf(
			"cannot generate values from text template: %w",
			err,
		)
	}
	err = file.Close()
	if err != nil {
		return fmt.Errorf(
			"cannot close file %v: %w",
			file,
			err,
		)
	}

	err = trimWhiteSpaceLines(filePath)
	if err != nil {
		return fmt.Errorf(
			"failed to trim white space lines from file: %w",
			err,
		)
	}
	return nil
}

func (g *ConfigGenerator) constructCfgOverride(
	ctx context.Context,
	cluster string,
	zone string,
	seed bool,
) CfgFileOverride {

	// default cfg file overrides for all clusters
	override := g.spec.ServiceSpec.DefaultOverride

	// cfg file overrides for zone
	zoneConfig := g.spec.ServiceSpec.Clusters[cluster].ZoneCfgOverride[zone]
	if zoneConfig.MonAddress != "" {
		override.MonAddress = zoneConfig.MonAddress
	}
	if zoneConfig.Domain != "" {
		override.Domain = zoneConfig.Domain
	}
	if zoneConfig.MonPort != "" {
		override.MonPort = zoneConfig.MonPort
	}
	if zoneConfig.IcPort != "" {
		override.IcPort = zoneConfig.IcPort
	}
	if zoneConfig.IcDiskAgentPort != "" {
		override.IcDiskAgentPort = zoneConfig.IcDiskAgentPort
	}
	if zoneConfig.VHostIcPort != "" {
		override.VHostIcPort = zoneConfig.VHostIcPort
	}

	// cfg file overrides for seed
	if seed {
		seedConfig := g.spec.ServiceSpec.Clusters[cluster].ZoneCfgOverrideSeed[zone]
		if seedConfig.MonAddress != "" {
			override.MonAddress = seedConfig.MonAddress
		}
		if seedConfig.Domain != "" {
			override.Domain = seedConfig.Domain
		}
		if seedConfig.MonPort != "" {
			override.MonPort = seedConfig.MonPort
		}
		if seedConfig.IcPort != "" {
			override.IcPort = seedConfig.IcPort
		}
	}
	override.LookupFunc = template.FuncMap{
		"lookup": func(key string, defaultValue string) string {
			return g.lookupCustomKey(ctx, key, defaultValue, cluster, zone, seed)
		},
	}
	return override
}

func (g *ConfigGenerator) lookupCustomKey(
	ctx context.Context,
	key string,
	defaultValue string,
	cluster string,
	zone string,
	seed bool,
) string {

	if seed {
		seedOverrides := g.spec.ServiceSpec.Clusters[cluster].ZoneCfgOverrideSeed[zone].CustomOverrides
		if value, ok := seedOverrides[key]; ok {
			return value
		}
	}

	zoneConfig := g.spec.ServiceSpec.Clusters[cluster].ZoneCfgOverride[zone]
	if value, ok := zoneConfig.CustomOverrides[key]; ok {
		return value
	}

	if value, ok := g.spec.ServiceSpec.DefaultOverride.CustomOverrides[key]; ok {
		return value
	}

	g.LogDbg(ctx, "key %v not found in custom overrides for cluster %v, zone %v", key, cluster, zone)
	return defaultValue
}

func (g *ConfigGenerator) generateConfigForCluster(
	ctx context.Context,
	target string,
	cluster string,
	zone string,
	seed bool,
) error {

	g.LogInfo(
		ctx,
		"Generating configs for cluster %v, zone %v, target %v",
		cluster,
		zone,
		target)

	for _, protobuf := range g.spec.ConfigMap {
		protobuf.Proto.Reset()
	}

	err := g.loadAllOverrides(ctx, target, cluster, zone, &g.spec.ConfigMap, seed)
	if err != nil {
		return fmt.Errorf(
			"failed to apply overrides for cluster %v: %w",
			cluster,
			err,
		)
	}

	err = g.dumpConfigs(
		ctx,
		&g.spec.ConfigMap,
		target,
		cluster,
		zone,
		seed)
	if err != nil {
		return fmt.Errorf(
			"failed to dump configs: %w",
			err,
		)
	}

	return nil
}

func contains(collection []string, target string) bool {
	for _, s := range collection {
		if s == target {
			return true
		}
	}
	return false
}

func (g *ConfigGenerator) Generate(ctx context.Context, whiteListCluster []string) error {
	g.LogInfo(
		ctx,
		"Start generation for service: %v",
		g.spec.ServiceSpec.ServiceName)

	genAllClusters := len(whiteListCluster) == 0
	for cluster, clusterConfig := range g.spec.ServiceSpec.Clusters {
		if !genAllClusters && !contains(whiteListCluster, cluster) {
			continue
		}
		for _, zone := range clusterConfig.Zones {
			for _, target := range clusterConfig.Targets {
				err := g.generateConfigForCluster(
					ctx,
					target,
					cluster,
					zone,
					false)
				if err != nil {
					return fmt.Errorf(
						"failed to generate configs for cluster %v, target %v: %w",
						cluster,
						target,
						err,
					)
				}
			}
			if g.spec.ServiceSpec.Clusters[cluster].GenerateSeed {
				err := g.generateConfigForCluster(
					ctx,
					g.spec.ServiceSpec.Clusters[cluster].TargetForSeed,
					cluster,
					zone,
					true)
				if err != nil {
					return fmt.Errorf(
						"failed to generate seed configs for cluster %v: %w",
						cluster,
						err,
					)
				}
			}
		}
	}
	return nil
}

func NewConfigGenerator(
	log nbs.Log,
	spec GeneratorSpec,
) *ConfigGenerator {

	return &ConfigGenerator{
		WithLog: logutil.WithLog{
			Log: log,
		},
		spec: spec,
	}
}
