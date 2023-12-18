package client

import (
	"crypto/rand"
	"fmt"
	"time"
)

////////////////////////////////////////////////////////////////////////////////

func createUUID() (string, error) {
	buff := make([]byte, 12)
	numRead, err := rand.Read(buff)

	if numRead != len(buff) || err != nil {
		return "", err
	}

	// generate 32 bits timestamp
	unix32bits := uint32(time.Now().UTC().Unix())

	uid := fmt.Sprintf(
		"%x-%x-%x-%x-%x-%x",
		unix32bits,
		buff[0:2],
		buff[2:4],
		buff[4:6],
		buff[6:8],
		buff[8:])

	return uid, nil
}
