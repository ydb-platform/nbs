package configurator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strings"
	"text/template"

	nbs "a.yandex-team.ru/cloud/blockstore/public/sdk/go/client"
	logutil "a.yandex-team.ru/cloud/storage/core/tools/common/go/log"

	"github.com/golang/protobuf/proto"
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
	Domain     string `yaml:"domain"`
	MonAddress string `yaml:"monAddress"`
	MonPort    string `yaml:"monPort"`
	IcPort     string `yaml:"icPort"`
}

type ZoneCfgFileOverrides struct {
	DefaultOverride CfgFileOverride            `yaml:"defaultOverride"`
	ZoneOverride    map[string]CfgFileOverride `yaml:"zoneOverride"`
}

type ClusterSpec struct {
	Targets                         []string             `yaml:"targets"`
	Zones                           []string             `yaml:"zones"`
	ZoneCfgOverride                 ZoneCfgFileOverrides `yaml:"zoneCfgOverride"`
	AdditionalFiles                 []string             `yaml:"additionalFiles"`
	AdditionalFilesPath             string               `yaml:"additionalFilesPath"`
	AdditionalFilesPathTargetPrefix string               `yaml:"additionalFilesPathTargetPrefix"`
	Configs                         ConfigsSpec          `yaml:"configs"`
	Values                          ValuesSpec           `yaml:"values"`
}

type Clusters map[string]ClusterSpec

type ServiceSpec struct {
	ServiceName  string              `yaml:"name"`
	CfgFileNames map[string][]string `yaml:"cfgFileNames"`
	Clusters     Clusters            `yaml:"clusters"`
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
	OverridesPath string,
	configProto *ConfigMap) error {

	for resultConfigFileName, configDesc := range *configProto {
		configProtoPath := path.Join(OverridesPath, configDesc.FileName)
		g.LogDbg(ctx, "Reading file %v", configProtoPath)

		configData, err := ioutil.ReadFile(configProtoPath)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf(
				"failed to read protobuf from config proto file %v: %w",
				configProtoPath,
				err,
			)
		}

		newConfigProto := proto.Clone(configDesc.Proto)
		err = proto.UnmarshalText(string(configData), newConfigProto)
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
	OverridesPath string,
	configMap *ConfigMap) error {

	err := g.updateConfigMapFromDir(ctx, OverridesPath, configMap)
	if err != nil {
		return err
	}

	return g.updateConfigMapFromDir(
		ctx,
		path.Join(OverridesPath, target),
		configMap)
}

func (g *ConfigGenerator) loadAllOverrides(
	ctx context.Context,
	target string,
	cluster string,
	zone string,
	configMap *ConfigMap) error {

	g.LogDbg(
		ctx,
		"Reading overrides for cluster %v, zone %v, target %v",
		cluster,
		zone,
		target)

	err := g.loadOverrides(
		ctx,
		target,
		path.Join(g.spec.OverridesPath, "common"),
		configMap)
	if err != nil {
		return err
	}

	err = g.loadOverrides(
		ctx,
		target,
		path.Join(g.spec.OverridesPath, cluster, "common"),
		configMap)
	if err != nil {
		return err
	}

	return g.loadOverrides(
		ctx,
		target,
		path.Join(g.spec.OverridesPath, cluster, zone),
		configMap)
}

func (g *ConfigGenerator) dumpConfigs(
	ctx context.Context,
	configs *ConfigMap,
	target string,
	cluster string,
	zone string,
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

		tmpl, err := template.New(cfgFile).Parse(string(cfgFileData))
		if err != nil {
			return fmt.Errorf(
				"failed to parse cfg file %v: %w",
				cfgFileData,
				err,
			)
		}

		override := g.constructCfgOverride(cluster, zone)
		var result bytes.Buffer
		err = tmpl.Execute(&result, override)
		if err != nil {
			return fmt.Errorf(
				"failed to generate cfg file %v: %w",
				cfgFile,
				err,
			)
		}
		resultConfigs = append(resultConfigs, ResultConfig{cfgFile, result.String()})
	}

	for _, fileName := range g.spec.ServiceSpec.Clusters[cluster].AdditionalFiles {
		filePath := path.Join(
			g.spec.ArcadiaPath,
			g.spec.ServiceSpec.Clusters[cluster].AdditionalFilesPath,
			zone,
			target,
			g.spec.ServiceSpec.Clusters[cluster].AdditionalFilesPathTargetPrefix,
			fileName)
		fileData, err := ioutil.ReadFile(filePath)
		if err != nil {
			return fmt.Errorf(
				"failed to read file %v: %w",
				filePath,
				err,
			)
		}
		resultConfigs = append(resultConfigs, ResultConfig{fileName, string(fileData)})
	}

	if g.spec.ServiceSpec.Clusters[cluster].Configs.Generate {
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
		err := g.dumpValues(
			ctx,
			resultConfigs,
			path.Join(
				g.spec.OverridesPath,
				g.spec.ServiceSpec.Clusters[cluster].Values.FileName),
			path.Join(
				g.spec.ArcadiaPath,
				g.spec.ServiceSpec.Clusters[cluster].Values.DumpPath,
				cluster,
				zone,
				target),
			target)
		if err != nil {
			return fmt.Errorf(
				"failed to generate values: %w",
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

func (g *ConfigGenerator) dumpValues(
	ctx context.Context,
	configs []ResultConfig,
	valuesTemplatePath string,
	configPath string,
	target string,
) error {

	g.LogDbg(ctx, "dump values to %v", configPath)

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
	err = os.MkdirAll(configPath, 0755)
	if err != nil {
		return fmt.Errorf(
			"cannot create directories: %w",
			err,
		)
	}
	file, err := os.OpenFile(
		path.Join(configPath, "values.yaml"),
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
	return nil
}

func (g *ConfigGenerator) constructCfgOverride(
	cluster string,
	zone string,
) CfgFileOverride {

	zoneConfig := g.spec.ServiceSpec.Clusters[cluster].ZoneCfgOverride
	override := zoneConfig.DefaultOverride

	if zoneConfig.ZoneOverride[zone].MonAddress != "" {
		override.MonAddress = zoneConfig.ZoneOverride[zone].MonAddress
	}

	if zoneConfig.ZoneOverride[zone].Domain != "" {
		override.Domain = zoneConfig.ZoneOverride[zone].Domain
	}

	if zoneConfig.ZoneOverride[zone].MonPort != "" {
		override.MonPort = zoneConfig.ZoneOverride[zone].MonPort
	}

	if zoneConfig.ZoneOverride[zone].IcPort != "" {
		override.IcPort = zoneConfig.ZoneOverride[zone].IcPort
	}

	return override
}

func (g *ConfigGenerator) generateConfigForCluster(
	ctx context.Context,
	target string,
	cluster string,
	zone string,
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

	err := g.loadAllOverrides(ctx, target, cluster, zone, &g.spec.ConfigMap)
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
		zone)
	if err != nil {
		return fmt.Errorf(
			"failed to dump configs: %w",
			err,
		)
	}

	return nil
}

func (g *ConfigGenerator) Generate(ctx context.Context) error {
	g.LogInfo(
		ctx,
		"Start generation for service: %v",
		g.spec.ServiceSpec.ServiceName)

	for cluster, clusterConfig := range g.spec.ServiceSpec.Clusters {
		for _, zone := range clusterConfig.Zones {
			for _, target := range clusterConfig.Targets {
				err := g.generateConfigForCluster(
					ctx,
					target,
					cluster,
					zone)
				if err != nil {
					return fmt.Errorf(
						"failed to generate configs for cluster %v, target %v: %w",
						cluster,
						target,
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
