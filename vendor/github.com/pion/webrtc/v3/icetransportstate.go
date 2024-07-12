// SPDX-FileCopyrightText: 2023 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

package webrtc

import "github.com/pion/ice/v2"

// ICETransportState represents the current state of the ICE transport.
type ICETransportState int

const (
    // ICETransportStateNew indicates the ICETransport is waiting
    // for remote candidates to be supplied.
    ICETransportStateNew = iota + 1

    // ICETransportStateChecking indicates the ICETransport has
    // received at least one remote candidate, and a local and remote
    // ICECandidateComplete dictionary was not added as the last candidate.
    ICETransportStateChecking

    // ICETransportStateConnected indicates the ICETransport has
    // received a response to an outgoing connectivity check, or has
    // received incoming DTLS/media after a successful response to an
    // incoming connectivity check, but is still checking other candidate
    // pairs to see if there is a better connection.
    ICETransportStateConnected

    // ICETransportStateCompleted indicates the ICETransport tested
    // all appropriate candidate pairs and at least one functioning
    // candidate pair has been found.
    ICETransportStateCompleted

    // ICETransportStateFailed indicates the ICETransport the last
    // candidate was added and all appropriate candidate pairs have either
    // failed connectivity checks or have lost consent.
    ICETransportStateFailed

    // ICETransportStateDisconnected indicates the ICETransport has received
    // at least one local and remote candidate, but the final candidate was
    // received yet and all appropriate candidate pairs thus far have been
    // tested and failed.
    ICETransportStateDisconnected

    // ICETransportStateClosed indicates the ICETransport has shut down
    // and is no longer responding to STUN requests.
    ICETransportStateClosed
)

const (
    ICETransportStateNewStr          = "new"
    ICETransportStateCheckingStr     = "checking"
    ICETransportStateConnectedStr    = "connected"
    ICETransportStateCompletedStr    = "completed"
    ICETransportStateFailedStr       = "failed"
    ICETransportStateDisconnectedStr = "disconnected"
    ICETransportStateClosedStr       = "closed"
)

func (c ICETransportState) String() string {
    switch c {
    case ICETransportStateNew:
        return ICETransportStateNewStr
    case ICETransportStateChecking:
        return ICETransportStateCheckingStr
    case ICETransportStateConnected:
        return ICETransportStateConnectedStr
    case ICETransportStateCompleted:
        return ICETransportStateCompletedStr
    case ICETransportStateFailed:
        return ICETransportStateFailedStr
    case ICETransportStateDisconnected:
        return ICETransportStateDisconnectedStr
    case ICETransportStateClosed:
        return ICETransportStateClosedStr
    default:
        return unknownStr
    }
}

func newICETransportStateFromICE(i ice.ConnectionState) ICETransportState {
    switch i {
    case ice.ConnectionStateNew:
        return ICETransportStateNew
    case ice.ConnectionStateChecking:
        return ICETransportStateChecking
    case ice.ConnectionStateConnected:
        return ICETransportStateConnected
    case ice.ConnectionStateCompleted:
        return ICETransportStateCompleted
    case ice.ConnectionStateFailed:
        return ICETransportStateFailed
    case ice.ConnectionStateDisconnected:
        return ICETransportStateDisconnected
    case ice.ConnectionStateClosed:
        return ICETransportStateClosed
    default:
        return ICETransportState(Unknown)
    }
}

func (c ICETransportState) toICE() ice.ConnectionState {
    switch c {
    case ICETransportStateNew:
        return ice.ConnectionStateNew
    case ICETransportStateChecking:
        return ice.ConnectionStateChecking
    case ICETransportStateConnected:
        return ice.ConnectionStateConnected
    case ICETransportStateCompleted:
        return ice.ConnectionStateCompleted
    case ICETransportStateFailed:
        return ice.ConnectionStateFailed
    case ICETransportStateDisconnected:
        return ice.ConnectionStateDisconnected
    case ICETransportStateClosed:
        return ice.ConnectionStateClosed
    default:
        return ice.ConnectionState(Unknown)
    }
}

func newICETransportState(raw string) ICETransportState {
    switch raw {
    case ICETransportStateNewStr:
        return ICETransportStateNew
    case ICETransportStateCheckingStr:
        return ICETransportStateChecking
    case ICETransportStateConnectedStr:
        return ICETransportStateConnected
    case ICETransportStateCompletedStr:
        return ICETransportStateCompleted
    case ICETransportStateFailedStr:
        return ICETransportStateFailed
    case ICETransportStateDisconnectedStr:
        return ICETransportStateDisconnected
    case ICETransportStateClosedStr:
        return ICETransportStateClosed
    default:
        return ICETransportState(Unknown)
    }
}

// MarshalText implements encoding.TextMarshaler
func (t ICETransportState) MarshalText() ([]byte, error) {
    return []byte(t.String()), nil
}

// UnmarshalText implements encoding.TextUnmarshaler
func (t *ICETransportState) UnmarshalText(b []byte) error {
    *t = newICETransportState(string(b))
    return nil
}
