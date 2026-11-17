package common

import "testing"

////////////////////////////////////////////////////////////////////////////////

func TestDisabledZones(t *testing.T) {

	testCases := []struct {
		name      string
		zones     []string
		zoneID    string
		disabled  bool
		nilPolicy bool
	}{
		{name: "nil list", zoneID: "zone-a", nilPolicy: true},
		{name: "empty list", zones: []string{}, zoneID: "zone-a", nilPolicy: true},
		{name: "disabled zone", zones: []string{"zone-a"}, zoneID: "zone-a", disabled: true},
		{name: "enabled zone", zones: []string{"zone-a"}, zoneID: "zone-b"},
		{name: "disabled shard", zones: []string{"zone-a-shard1"}, zoneID: "zone-a-shard1", disabled: true},
		{name: "zone does not disable shard", zones: []string{"zone-a"}, zoneID: "zone-a-shard1"},
		{name: "shard does not disable zone", zones: []string{"zone-a-shard1"}, zoneID: "zone-a"},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			zones := NewDisabledZones(testCase.zones)
			if (zones == nil) != testCase.nilPolicy {
				t.Fatalf("NewDisabledZones(%v) is nil = %v, want %v", testCase.zones, zones == nil, testCase.nilPolicy)
			}
			if disabled := zones.IsDisabled(testCase.zoneID); disabled != testCase.disabled {
				t.Errorf("IsDisabled(%q) = %v, want %v", testCase.zoneID, disabled, testCase.disabled)
			}
		})
	}

	t.Run("missing policy", func(t *testing.T) {
		var zones *DisabledZones
		if zones.IsDisabled("zone-a") {
			t.Fatal("a missing policy must not disable a zone")
		}
	})
}
