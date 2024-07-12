package datachannel

import (
    "testing"

    "github.com/stretchr/testify/assert"
)

func TestChannelOpenMarshal(t *testing.T) {
    msg := channelOpen{
        ChannelType:          ChannelTypeReliable,
        Priority:             0,
        ReliabilityParameter: 0,

        Label:    []byte("foo"),
        Protocol: []byte("bar"),
    }

    rawMsg, err := msg.Marshal()
    if err != nil {
        t.Errorf("Failed to marshal: %v", err)
        return
    }

    result := []byte{
        0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x03, 0x00, 0x03, 0x66, 0x6f, 0x6f, 0x62, 0x61, 0x72,
    }

    if len(rawMsg) != len(result) {
        t.Errorf("%q != %q", rawMsg, result)
        return
    }

    for i, v := range rawMsg {
        if v != result[i] {
            t.Errorf("%q != %q", rawMsg, result)
            break
        }
    }
}

func TestChannelAckMarshal(t *testing.T) {
    msg := channelAck{}
    rawMsg, err := msg.Marshal()
    if err != nil {
        t.Errorf("Failed to marshal: %v", err)
        return
    }
    result := []byte{0x02, 0x00, 0x00, 0x00}

    if len(rawMsg) != len(result) {
        t.Errorf("%q != %q", rawMsg, result)
        return
    }

    for i, v := range rawMsg {
        if v != result[i] {
            t.Errorf("%q != %q", rawMsg, result)
            break
        }
    }
}

func TestChannelOpenUnmarshal(t *testing.T) {
    rawMsg := []byte{
        0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x03, 0x00, 0x03, 0x66, 0x6f, 0x6f, 0x62, 0x61, 0x72,
    }

    msgUncast, err := parse(rawMsg)

    msg, ok := msgUncast.(*channelOpen)
    assert.True(t, ok, "Failed to cast to ChannelOpen")

    assert.NoError(t, err, "Unmarshal failed, ChannelOpen")
    assert.Equal(t, msg.ChannelType, ChannelTypeReliable, "ChannelType should be 0")
    assert.Equal(t, msg.Priority, uint16(0), "Priority should be 0")
    assert.Equal(t, msg.ReliabilityParameter, uint32(0), "ReliabilityParameter should be 0")
    assert.Equal(t, msg.Label, []uint8("foo"), "msg Label should be 'foo'")
    assert.Equal(t, msg.Protocol, []uint8("bar"), "msg protocol should be 'bar'")
}

func TestChannelAckUnmarshal(t *testing.T) {
    rawMsg := []byte{0x02}
    msgUncast, err := parse(rawMsg)
    if err != nil {
        t.Errorf("Failed to parse: %v", err)
        return
    }

    _, ok := msgUncast.(*channelAck)
    assert.True(t, ok, "Failed to cast to ChannelAck")
}
