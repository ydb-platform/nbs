package backup

import (
	"testing"

	"github.com/stretchr/testify/require"
)

////////////////////////////////////////////////////////////////////////////////

func TestNewS3FailsOnWrongKekSize(t *testing.T) {
	_, err := NewS3(nil, "bucket", "", "kek1", make([]byte, 16))
	require.Error(t, err)
}

func TestSealAndOpen(t *testing.T) {
	aead, err := newAEAD(make([]byte, keySize))
	require.NoError(t, err)

	data := []byte("chunk data")
	sealed, err := seal(aead, data, []byte("chunks/chunk1"))
	require.NoError(t, err)
	require.Len(t, sealed, aead.NonceSize()+len(data)+aead.Overhead())
	require.NotContains(t, string(sealed), string(data))

	opened, err := open(aead, sealed, []byte("chunks/chunk1"))
	require.NoError(t, err)
	require.Equal(t, data, opened)

	resealed, err := seal(aead, data, []byte("chunks/chunk1"))
	require.NoError(t, err)
	require.NotEqual(t, sealed, resealed)

	_, err = open(aead, sealed, []byte("chunks/chunk2"))
	require.Error(t, err)

	sealed[len(sealed)-1] ^= 1
	_, err = open(aead, sealed, []byte("chunks/chunk1"))
	require.Error(t, err)

	_, err = open(aead, sealed[:aead.NonceSize()], nil)
	require.Error(t, err)
}

func TestEncryptedDEK(t *testing.T) {
	kek := make([]byte, keySize)
	kek[0] = 1

	backupS3, err := NewS3(nil, "bucket", "", "kek1", kek)
	require.NoError(t, err)

	encryptedDEK, err := backupS3.NewEncryptedDEK()
	require.NoError(t, err)
	require.Len(t, encryptedDEK, 12+keySize+16)

	dek, err := backupS3.openDEK(encryptedDEK)
	require.NoError(t, err)

	sealed, err := seal(dek, []byte("data"), nil)
	require.NoError(t, err)

	opened, err := open(dek, sealed, nil)
	require.NoError(t, err)
	require.Equal(t, []byte("data"), opened)

	otherKEK := make([]byte, keySize)
	other, err := NewS3(nil, "bucket", "", "kek2", otherKEK)
	require.NoError(t, err)

	_, err = other.openDEK(encryptedDEK)
	require.Error(t, err)
}
