package portmanager

import "testing"

type PortManagerT struct {
	base *PortManager
	tb   testing.TB
}

// NewT creates new PortManager that automatically releases ports back to the CI
// when test is finished.
func NewT(tb testing.TB) *PortManagerT {
	pm, err := New()
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() {
		err := pm.Cleanup()
		if err != nil {
			tb.Fatal(err)
		}
	})
	return &PortManagerT{
		base: pm,
		tb:   tb,
	}
}

// GetPort allocates new free port using base PortManager
// Errors are handled by calling (testing.TB).Fatalf.
func (pm *PortManagerT) GetPort(defaultPort ...int) int {
	port, err := pm.base.GetPort(defaultPort...)
	if err != nil {
		pm.tb.Fatal(err)
	}
	return port
}

// GetSequentialPorts allocates new sequential ports using base PortManager
// Errors are handled by calling (testing.TB).Fatalf.
func (pm *PortManagerT) GetSequentialPorts(count int, defaultPort ...int) []int {
	ports, err := pm.base.GetSequentialPorts(count, defaultPort...)
	if err != nil {
		pm.tb.Fatal(err)
	}
	return ports
}
