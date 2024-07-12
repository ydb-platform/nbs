// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package extension

import (
    "context"
    "testing"

    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"

    "go.opentelemetry.io/collector/component"
    "go.opentelemetry.io/collector/component/componenttest"
)

type nopExtension struct {
    component.StartFunc
    component.ShutdownFunc
    CreateSettings
}

func TestNewFactory(t *testing.T) {
    const typeStr = "test"
    defaultCfg := struct{}{}
    nopExtensionInstance := new(nopExtension)

    factory := NewFactory(
        typeStr,
        func() component.Config { return &defaultCfg },
        func(ctx context.Context, settings CreateSettings, extension component.Config) (Extension, error) {
            return nopExtensionInstance, nil
        },
        component.StabilityLevelDevelopment)
    assert.EqualValues(t, typeStr, factory.Type())
    assert.EqualValues(t, &defaultCfg, factory.CreateDefaultConfig())

    assert.Equal(t, component.StabilityLevelDevelopment, factory.ExtensionStability())
    ext, err := factory.CreateExtension(context.Background(), CreateSettings{}, &defaultCfg)
    assert.NoError(t, err)
    assert.Same(t, nopExtensionInstance, ext)
}

func TestMakeFactoryMap(t *testing.T) {
    type testCase struct {
        name string
        in   []Factory
        out  map[component.Type]Factory
    }

    p1 := NewFactory("p1", nil, nil, component.StabilityLevelAlpha)
    p2 := NewFactory("p2", nil, nil, component.StabilityLevelAlpha)
    testCases := []testCase{
        {
            name: "different names",
            in:   []Factory{p1, p2},
            out: map[component.Type]Factory{
                p1.Type(): p1,
                p2.Type(): p2,
            },
        },
        {
            name: "same name",
            in:   []Factory{p1, p2, NewFactory("p1", nil, nil, component.StabilityLevelAlpha)},
        },
    }
    for i := range testCases {
        tt := testCases[i]
        t.Run(tt.name, func(t *testing.T) {
            out, err := MakeFactoryMap(tt.in...)
            if tt.out == nil {
                assert.Error(t, err)
                return
            }
            assert.NoError(t, err)
            assert.Equal(t, tt.out, out)
        })
    }
}

func TestBuilder(t *testing.T) {
    const typeStr = "test"
    defaultCfg := struct{}{}
    testID := component.NewID(typeStr)
    unknownID := component.NewID("unknown")

    factories, err := MakeFactoryMap([]Factory{
        NewFactory(
            typeStr,
            func() component.Config { return &defaultCfg },
            func(ctx context.Context, settings CreateSettings, extension component.Config) (Extension, error) {
                return nopExtension{CreateSettings: settings}, nil
            },
            component.StabilityLevelDevelopment),
    }...)
    require.NoError(t, err)

    cfgs := map[component.ID]component.Config{testID: defaultCfg, unknownID: defaultCfg}
    b := NewBuilder(cfgs, factories)

    e, err := b.Create(context.Background(), createSettings(testID))
    assert.NoError(t, err)
    assert.NotNil(t, e)

    // Check that the extension has access to the resource attributes.
    nop, ok := e.(nopExtension)
    assert.True(t, ok)
    assert.Equal(t, nop.CreateSettings.Resource.Attributes().Len(), 0)

    missingType, err := b.Create(context.Background(), createSettings(unknownID))
    assert.EqualError(t, err, "extension factory not available for: \"unknown\"")
    assert.Nil(t, missingType)

    missingCfg, err := b.Create(context.Background(), createSettings(component.NewIDWithName(typeStr, "foo")))
    assert.EqualError(t, err, "extension \"test/foo\" is not configured")
    assert.Nil(t, missingCfg)
}

func TestBuilderFactory(t *testing.T) {
    factories, err := MakeFactoryMap([]Factory{NewFactory("foo", nil, nil, component.StabilityLevelDevelopment)}...)
    require.NoError(t, err)

    cfgs := map[component.ID]component.Config{component.NewID("foo"): struct{}{}}
    b := NewBuilder(cfgs, factories)

    assert.NotNil(t, b.Factory(component.NewID("foo").Type()))
    assert.Nil(t, b.Factory(component.NewID("bar").Type()))
}

func createSettings(id component.ID) CreateSettings {
    return CreateSettings{
        ID:                id,
        TelemetrySettings: componenttest.NewNopTelemetrySettings(),
        BuildInfo:         component.NewDefaultBuildInfo(),
    }
}
