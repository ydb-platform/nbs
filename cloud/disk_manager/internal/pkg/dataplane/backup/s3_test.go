package backup

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

func TestNewS3FailsOnWrongKekSize(t *testing.T) {
	_, err := NewS3(nil, "bucket", "", "kek1", make([]byte, 16))
	require.Error(t, err)
}

func TestNewS3FailsOnEmptyKekID(t *testing.T) {
	_, err := NewS3(nil, "bucket", "", "", make([]byte, keySize))
	require.Error(t, err)
}

func TestNewS3WithoutKek(t *testing.T) {
	backupS3, err := NewS3(nil, "bucket", "", "", nil)
	require.NoError(t, err)

	dek, err := backupS3.EnsureEncryptedDEK(nil)
	require.NoError(t, err)
	require.Empty(t, dek)
	require.NoError(t, backupS3.CheckEncryptedDEK(nil))

	_, err = backupS3.NewEncryptedDEK()
	require.Error(t, err)

	err = backupS3.CheckEncryptedDEK([]byte("dek"))
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

	_, err = backupS3.EnsureEncryptedDEK([]byte("dek"))
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

	err = backupS3.PutObject(
		context.Background(),
		"chunks/chunk1",
		[]byte("dek"),
		persistence.S3Object{Data: []byte("abc")},
	)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))
}

func TestNewS3FailsWhenOnlyKekIDIsSet(t *testing.T) {
	_, err := NewS3(nil, "bucket", "", "kek1", nil)
	require.Error(t, err)
}

func TestNewS3TrimsKekWhitespace(t *testing.T) {
	kek := make([]byte, keySize)
	kek[0] = 1
	kek[keySize-1] = '\n'

	_, err := NewS3(nil, "bucket", "", "kek1", kek)
	require.NoError(t, err)

	clean := make([]byte, keySize)
	clean[0] = 1
	padded := append(append([]byte{'\n'}, clean...), '\n')
	backupS3, err := NewS3(nil, "bucket", "", "kek1", padded)
	require.NoError(t, err)

	encryptedDEK, err := backupS3.NewEncryptedDEK()
	require.NoError(t, err)

	expected, err := NewS3(nil, "bucket", "", "kek1", clean)
	require.NoError(t, err)

	_, err = expected.decryptDEK(encryptedDEK)
	require.NoError(t, err)
}

func TestEncryptAndDecrypt(t *testing.T) {
	aead, err := newAEAD(make([]byte, keySize))
	require.NoError(t, err)

	data := []byte("chunk data")
	sealed, err := encrypt(aead, data, []byte("chunks/chunk1"))
	require.NoError(t, err)
	require.Len(t, sealed, aead.NonceSize()+len(data)+aead.Overhead())
	require.NotContains(t, string(sealed), string(data))

	opened, err := decrypt(aead, sealed, []byte("chunks/chunk1"))
	require.NoError(t, err)
	require.Equal(t, data, opened)

	resealed, err := encrypt(aead, data, []byte("chunks/chunk1"))
	require.NoError(t, err)
	require.NotEqual(t, sealed, resealed)

	_, err = decrypt(aead, sealed, []byte("chunks/chunk2"))
	require.Error(t, err)

	sealed[len(sealed)-1] ^= 1
	_, err = decrypt(aead, sealed, []byte("chunks/chunk1"))
	require.Error(t, err)

	_, err = decrypt(aead, sealed[:aead.NonceSize()], nil)
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

	dek, err := backupS3.decryptDEK(encryptedDEK)
	require.NoError(t, err)

	sealed, err := encrypt(dek, []byte("data"), nil)
	require.NoError(t, err)

	opened, err := decrypt(dek, sealed, nil)
	require.NoError(t, err)
	require.Equal(t, []byte("data"), opened)

	otherKEK := make([]byte, keySize)
	other, err := NewS3(nil, "bucket", "", "kek2", otherKEK)
	require.NoError(t, err)

	_, err = other.decryptDEK(encryptedDEK)
	require.Error(t, err)

	sameKey, err := NewS3(nil, "bucket", "", "kek2", kek)
	require.NoError(t, err)

	_, err = sameKey.decryptDEK(encryptedDEK)
	require.Error(t, err)
}

func TestCheckEncryptedDEK(t *testing.T) {
	kek := make([]byte, keySize)
	kek[0] = 1

	backupS3, err := NewS3(nil, "bucket", "", "kek1", kek)
	require.NoError(t, err)

	encryptedDEK, err := backupS3.NewEncryptedDEK()
	require.NoError(t, err)
	require.NoError(t, backupS3.CheckEncryptedDEK(encryptedDEK))

	err = backupS3.CheckEncryptedDEK([]byte("bad"))
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))
}
