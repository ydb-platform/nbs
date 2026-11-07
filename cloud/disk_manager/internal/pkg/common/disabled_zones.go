package common

////////////////////////////////////////////////////////////////////////////////

type DisabledZones struct {
	zones []string
}

func NewDisabledZones(zones []string) *DisabledZones {
	if len(zones) == 0 {
		return nil
	}
	return &DisabledZones{zones: zones}
}

func (z *DisabledZones) IsDisabled(zoneID string) bool {
	return z != nil && Find(z.zones, zoneID)
}
