// Package portmanager implements port allocator for arcadia.
//
// Port manager coordinates port allocation using directory
// specified in $PORT_SYNC_PATH environment variable.
//
// Protocol is compatible with Python and C++ implementation.
//
// Allocating port with ":0" works, as long as you don't release
// that port to the operating system. If you release port and later
// try to reacquire the same port, another test process may come in and
// snatch it.
package portmanager

import (
	"fmt"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"sync"
)

const (
	maxRetries = 1024

	portMax                = (1 << 16) - 1
	portIANAEphemeralStart = (1 << 15) + (1 << 14)
)

// New creates new PortManager
// Cleanup function needs to be invoked manually to release ports acquired by GetPort
func New() (*PortManager, error) {
	pm := &PortManager{
		portSyncDir:   os.Getenv("PORT_SYNC_PATH"),
		noRandomPorts: os.Getenv("NO_RANDOM_PORTS") != "",

		lockedPorts: make(map[int]*os.File),
	}

	var err error
	pm.rangeStart, pm.rangeEnd, err = getSafePortRange()
	if err != nil {
		return nil, fmt.Errorf("failed to determine safe port range: %v", err)
	}

	return pm, nil
}

func getSafePortRange() (start, end int, err error) {
	explicitRange := os.Getenv("VALID_PORT_RANGE")
	if explicitRange != "" {
		_, err = fmt.Sscanf(explicitRange, "%d:%d", &start, &end)
		return
	}

	ephemeralStart, _, err := getEphemeralPortRange()
	if err != nil {
		return 0, 0, err
	}

	// Assume ephemeral range is within [1024, 65536).
	start = 1024
	end = ephemeralStart - 1

	return
}

type PortManager struct {
	rangeStart, rangeEnd int

	portSyncDir   string
	noRandomPorts bool

	mu          sync.Mutex
	lockedPorts map[int]*os.File
}

// Cleanup releases all allocated ports
// If any errors happen during the release, returns arbitary error
func (pm *PortManager) Cleanup() error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	var err error
	for port := range pm.lockedPorts {
		err = pm.releasePort(port)
	}
	return err
}

func (pm *PortManager) acquirePort(port int) error {
	if pm.portSyncDir == "" {
		return nil
	}

	lockPath := filepath.Join(pm.portSyncDir, fmt.Sprint(port))
	file, err := os.Create(lockPath)
	if err != nil {
		return fmt.Errorf("create lockfile: %w", err)
	}

	if err := tryLockFile(file); err != nil {
		_ = file.Close()
		return fmt.Errorf("try lock file: %w", err)
	}

	pm.lockedPorts[port] = file
	return nil
}

func (pm *PortManager) releasePort(port int) error {
	if pm.portSyncDir == "" {
		return nil
	}

	file := pm.lockedPorts[port]
	if file == nil {
		return fmt.Errorf("port is not locked")
	}

	if err := unlockFile(file); err != nil {
		return err
	}

	if err := file.Close(); err != nil {
		return err
	}

	delete(pm.lockedPorts, port)
	return nil
}

// GetPort allocates new free port.
// Optional defaultPort is used when --no-random-ports option is passed to ya make.
func (pm *PortManager) GetPort(defaultPort ...int) (int, error) {
	if pm.noRandomPorts && len(defaultPort) != 0 {
		return defaultPort[0], nil
	}

	pm.mu.Lock()
	defer pm.mu.Unlock()

	var err error
	for i := 0; i < maxRetries; i++ {
		randomPort := rand.Intn(pm.rangeEnd-pm.rangeStart) + pm.rangeStart

		var lsn net.Listener
		lsn, err = net.Listen("tcp", fmt.Sprintf(":%d", randomPort))
		if err != nil {
			continue
		}
		_ = lsn.Close()

		var pkt net.PacketConn
		pkt, err = net.ListenPacket("udp", fmt.Sprintf(":%d", randomPort))
		if err != nil {
			continue
		}
		_ = pkt.Close()

		if err = pm.acquirePort(randomPort); err != nil {
			continue
		}

		return randomPort, nil
	}

	return 0, fmt.Errorf("failed to allocate free port after %d iterations: %v", maxRetries, err)
}
