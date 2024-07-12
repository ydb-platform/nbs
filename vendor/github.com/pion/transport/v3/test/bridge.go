// SPDX-FileCopyrightText: 2023 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

package test

import (
    "errors"
    "fmt"
    "io"
    "math/rand"
    "net"
    "sync"
    "time"

    "github.com/pion/transport/v3/deadline"
)

const (
    tickWait  = 10 * time.Microsecond
    udpString = "udp"
)

var (
    errIOTimeout           = errors.New("i/o timeout")
    errBridgeConnClosed    = errors.New("bridgeConn closed")
    errBridgeAlreadyClosed = errors.New("bridge has already been closed")
    errInverseArrayWithOne = errors.New("inverse requires more than one item in the array")
    errBadLossChanceRange  = errors.New("loss must be < 100 && > 0")
)

type bridgeConnAddr int

func (a bridgeConnAddr) Network() string {
    return udpString
}

func (a bridgeConnAddr) String() string {
    return fmt.Sprintf("a%d", a)
}

// bridgeConn is a net.Conn that represents an endpoint of the bridge.
type bridgeConn struct {
    br            *Bridge
    id            int
    closeReq      bool
    closing       bool
    closed        bool
    mutex         sync.RWMutex
    readCh        chan []byte
    lossChance    int
    readDeadline  *deadline.Deadline
    writeDeadline *deadline.Deadline
}

type netError struct {
    error
    timeout, temporary bool
}

func (e *netError) Timeout() bool {
    return e.timeout
}

func (e *netError) Temporary() bool {
    return e.temporary
}

// Read reads data, block until the data becomes available.
func (conn *bridgeConn) Read(b []byte) (int, error) {
    select {
    case <-conn.readDeadline.Done():
        return 0, &netError{errIOTimeout, true, true}
    default:
    }

    select {
    case data, ok := <-conn.readCh:
        if !ok {
            return 0, io.EOF
        }
        n := copy(b, data)
        return n, nil
    case <-conn.readDeadline.Done():
        return 0, &netError{errIOTimeout, true, true}
    }
}

// Write writes data to the bridge.
func (conn *bridgeConn) Write(b []byte) (int, error) {
    select {
    case <-conn.writeDeadline.Done():
        return 0, &netError{errIOTimeout, true, true}
    default:
    }

    if rand.Intn(100) < conn.lossChance { //nolint:gosec
        return len(b), nil
    }

    if !conn.br.Push(b, conn.id) {
        return 0, &netError{errBridgeConnClosed, false, false}
    }
    return len(b), nil
}

// Close closes the bridge (releases resources used).
func (conn *bridgeConn) Close() error {
    conn.mutex.Lock()
    defer conn.mutex.Unlock()

    if conn.closeReq {
        return &netError{errBridgeAlreadyClosed, false, false}
    }

    conn.closeReq = true
    conn.closing = true
    return nil
}

// LocalAddr is not used
func (conn *bridgeConn) LocalAddr() net.Addr {
    return bridgeConnAddr(conn.id)
}

// RemoteAddr is not used
func (conn *bridgeConn) RemoteAddr() net.Addr { return nil }

// SetDeadline sets deadline of Read/Write operation.
// Setting zero means no deadline.
func (conn *bridgeConn) SetDeadline(t time.Time) error {
    conn.writeDeadline.Set(t)
    conn.readDeadline.Set(t)
    return nil
}

// SetReadDeadline sets deadline of Read operation.
// Setting zero means no deadline.
func (conn *bridgeConn) SetReadDeadline(t time.Time) error {
    conn.readDeadline.Set(t)
    return nil
}

// SetWriteDeadline sets deadline of Write operation.
// Setting zero means no deadline.
func (conn *bridgeConn) SetWriteDeadline(t time.Time) error {
    conn.writeDeadline.Set(t)
    return nil
}

func (conn *bridgeConn) isClosed() bool {
    conn.mutex.RLock()
    defer conn.mutex.RUnlock()

    return conn.closed
}

// Bridge represents a network between the two endpoints.
type Bridge struct {
    mutex sync.RWMutex
    conn0 *bridgeConn
    conn1 *bridgeConn

    queue0to1 [][]byte
    queue1to0 [][]byte

    dropNWrites0    int
    dropNWrites1    int
    reorderNWrites0 int
    reorderNWrites1 int
    stack0          [][]byte
    stack1          [][]byte
    filterCB0       func([]byte) bool
    filterCB1       func([]byte) bool
    err             error // last error
}

func inverse(s [][]byte) error {
    if len(s) < 2 {
        return errInverseArrayWithOne
    }

    for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
        s[i], s[j] = s[j], s[i]
    }
    return nil
}

// drop n packets from the slice starting from offset
func drop(s [][]byte, offset, n int) [][]byte {
    if offset+n > len(s) {
        n = len(s) - offset
    }
    return append(s[:offset], s[offset+n:]...)
}

// NewBridge creates a new bridge with two endpoints.
func NewBridge() *Bridge {
    br := &Bridge{
        queue0to1: make([][]byte, 0),
        queue1to0: make([][]byte, 0),
    }

    br.conn0 = &bridgeConn{
        br:            br,
        id:            0,
        readCh:        make(chan []byte),
        readDeadline:  deadline.New(),
        writeDeadline: deadline.New(),
    }
    br.conn1 = &bridgeConn{
        br:            br,
        id:            1,
        readCh:        make(chan []byte),
        readDeadline:  deadline.New(),
        writeDeadline: deadline.New(),
    }

    return br
}

// GetConn0 returns an endpoint of the bridge, conn0.
func (br *Bridge) GetConn0() net.Conn {
    return br.conn0
}

// GetConn1 returns an endpoint of the bridge, conn1.
func (br *Bridge) GetConn1() net.Conn {
    return br.conn1
}

// Len returns number of queued packets.
func (br *Bridge) Len(fromID int) int {
    br.mutex.Lock()
    defer br.mutex.Unlock()

    if fromID == 0 {
        return len(br.queue0to1)
    }
    return len(br.queue1to0)
}

// Push pushes a packet into the specified queue.
func (br *Bridge) Push(packet []byte, fromID int) bool { //nolint:gocognit
    d := make([]byte, len(packet))
    copy(d, packet)

    // Push rate should be limited as same as Tick rate.
    // Otherwise, queue grows too fast on free running Write.
    time.Sleep(tickWait)

    br.mutex.Lock()
    defer br.mutex.Unlock()

    br.conn0.mutex.Lock()
    br.conn1.mutex.Lock()
    closing0 := br.conn0.closing
    closing1 := br.conn1.closing
    br.conn1.mutex.Unlock()
    br.conn0.mutex.Unlock()

    if closing0 || closing1 {
        if fromID == 0 && closing0 {
            return false
        }
        if fromID == 1 && closing1 {
            return false
        }
        return true
    }

    if fromID == 0 {
        switch {
        case br.dropNWrites0 > 0:
            br.dropNWrites0--
            // fmt.Printf("br: dropped a packet of size %d (rem: %d for q0)\n", len(d), br.dropNWrites0) // nolint
        case br.reorderNWrites0 > 0:
            br.reorderNWrites0--
            br.stack0 = append(br.stack0, d)
            // fmt.Printf("stack0 size: %d\n", len(br.stack0)) // nolint
            if br.reorderNWrites0 == 0 {
                if err := inverse(br.stack0); err == nil {
                    // fmt.Printf("stack0 reordered!\n") // nolint
                    br.queue0to1 = append(br.queue0to1, br.stack0...)
                } else {
                    br.err = err
                }
            }
        case br.filterCB0 != nil && !br.filterCB0(d):
            // fmt.Printf("br: filtered out a packet of size %d (q0)\n", len(d)) // nolint
        default:
            // fmt.Printf("br: routed a packet of size %d (q0)\n", len(d)) // nolint
            br.queue0to1 = append(br.queue0to1, d)
        }
    } else {
        switch {
        case br.dropNWrites1 > 0:
            br.dropNWrites1--
            // fmt.Printf("br: dropped a packet of size %d (rem: %d for q1)\n", len(d), br.dropNWrites0) // nolint
        case br.reorderNWrites1 > 0:
            br.reorderNWrites1--
            br.stack1 = append(br.stack1, d)
            if br.reorderNWrites1 == 0 {
                if err := inverse(br.stack1); err != nil {
                    br.err = err
                }
                br.queue1to0 = append(br.queue1to0, br.stack1...)
            }
        case br.filterCB1 != nil && !br.filterCB1(d):
            // fmt.Printf("br: filtered out a packet of size %d (q1)\n", len(d)) // nolint
        default:
            // fmt.Printf("br: routed a packet of size %d (q1)\n", len(d)) // nolint
            br.queue1to0 = append(br.queue1to0, d)
        }
    }
    return true
}

// Reorder inverses the order of packets currently in the specified queue.
func (br *Bridge) Reorder(fromID int) error {
    br.mutex.Lock()
    defer br.mutex.Unlock()

    if fromID == 0 {
        return inverse(br.queue0to1)
    }
    return inverse(br.queue1to0)
}

// Drop drops the specified number of packets from the given offset index
// of the specified queue.
func (br *Bridge) Drop(fromID, offset, n int) {
    br.mutex.Lock()
    defer br.mutex.Unlock()

    if fromID == 0 {
        br.queue0to1 = drop(br.queue0to1, offset, n)
    } else {
        br.queue1to0 = drop(br.queue1to0, offset, n)
    }
}

// DropNextNWrites drops the next n packets that will be written
// to the specified queue.
func (br *Bridge) DropNextNWrites(fromID, n int) {
    br.mutex.Lock()
    defer br.mutex.Unlock()

    if fromID == 0 {
        br.dropNWrites0 = n
    } else {
        br.dropNWrites1 = n
    }
}

// ReorderNextNWrites drops the next n packets that will be written
// to the specified queue.
func (br *Bridge) ReorderNextNWrites(fromID, n int) {
    br.mutex.Lock()
    defer br.mutex.Unlock()

    if fromID == 0 {
        br.reorderNWrites0 = n
    } else {
        br.reorderNWrites1 = n
    }
}

func (br *Bridge) clear() {
    br.mutex.Lock()
    defer br.mutex.Unlock()

    br.queue1to0 = nil
    br.queue0to1 = nil
}

// Tick attempts to hand a packet from the queue for each directions, to readers,
// if there are waiting on the queue. If there's no reader, it will return
// immediately.
func (br *Bridge) Tick() int {
    br.mutex.Lock()
    defer br.mutex.Unlock()

    br.conn0.mutex.Lock()
    if br.conn0.closing && !br.conn0.closed && len(br.queue1to0) == 0 {
        br.conn0.closed = true
        close(br.conn0.readCh)
    }
    br.conn0.mutex.Unlock()
    br.conn1.mutex.Lock()
    if br.conn1.closing && !br.conn1.closed && len(br.queue0to1) == 0 {
        br.conn1.closed = true
        close(br.conn1.readCh)
    }
    br.conn1.mutex.Unlock()

    var n int
    if len(br.queue0to1) > 0 && !br.conn1.isClosed() {
        select {
        case br.conn1.readCh <- br.queue0to1[0]:
            n++
            // fmt.Printf("conn1 received data (%d bytes)\n", len(br.queue0to1[0])) // nolint
            br.queue0to1 = br.queue0to1[1:]
        default:
        }
    }

    if len(br.queue1to0) > 0 && !br.conn0.isClosed() {
        select {
        case br.conn0.readCh <- br.queue1to0[0]:
            n++
            // fmt.Printf("conn0 received data (%d bytes)\n", len(br.queue1to0[0])) // nolint
            br.queue1to0 = br.queue1to0[1:]
        default:
        }
    }

    return n
}

// Process repeats tick() calls until no more outstanding packet in the queues.
func (br *Bridge) Process() {
    for {
        time.Sleep(tickWait)
        br.Tick()
        if br.Len(0) == 0 && br.Len(1) == 0 {
            break
        }
    }
}

// SetLossChance sets the probability of writes being discard (to introduce artificial loss)
func (br *Bridge) SetLossChance(chance int) error {
    if chance > 100 || chance < 0 {
        return errBadLossChanceRange
    }

    rand.Seed(time.Now().UTC().UnixNano())
    br.conn0.lossChance = chance
    br.conn1.lossChance = chance
    return nil
}

// Filter filters (drops) packets based on return value of the given callback.
func (br *Bridge) Filter(fromID int, cb func([]byte) bool) {
    br.mutex.Lock()
    defer br.mutex.Unlock()

    if fromID == 0 {
        br.filterCB0 = cb
    } else {
        br.filterCB1 = cb
    }
}
