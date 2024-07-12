// Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////////

package hybrid

import (
    "errors"

    "google.golang.org/protobuf/proto"
    "github.com/google/tink/go/core/registry"
    "github.com/google/tink/go/hybrid/internal/hpke"
    "github.com/google/tink/go/keyset"
    hpkepb "github.com/google/tink/go/proto/hpke_go_proto"
    tinkpb "github.com/google/tink/go/proto/tink_go_proto"
)

const (
    // maxSupportedHPKEPublicKeyVersion is the max supported public key version.
    // It must be incremented when support for new versions are implemented.
    maxSupportedHPKEPublicKeyVersion = 0
    hpkePublicKeyTypeURL             = "type.googleapis.com/google.crypto.tink.HpkePublicKey"
)

var (
    errInvalidHPKEPublicKey = errors.New("invalid HPKE public key")
    errNotSupportedOnHPKE   = errors.New("not supported on HPKE public key manager")
)

// hpkePublicKeyManager implements the KeyManager interface for HybridEncrypt.
type hpkePublicKeyManager struct{}

var _ registry.KeyManager = (*hpkePublicKeyManager)(nil)

func (p *hpkePublicKeyManager) Primitive(serializedKey []byte) (interface{}, error) {
    if len(serializedKey) == 0 {
        return nil, errInvalidHPKEPublicKey
    }
    key := new(hpkepb.HpkePublicKey)
    if err := proto.Unmarshal(serializedKey, key); err != nil {
        return nil, errInvalidHPKEPublicKey
    }
    if err := keyset.ValidateKeyVersion(key.GetVersion(), maxSupportedHPKEPublicKeyVersion); err != nil {
        return nil, err
    }
    return hpke.NewEncrypt(key)
}

func (p *hpkePublicKeyManager) DoesSupport(typeURL string) bool {
    return typeURL == hpkePublicKeyTypeURL
}

func (p *hpkePublicKeyManager) TypeURL() string {
    return hpkePublicKeyTypeURL
}

func (p *hpkePublicKeyManager) NewKey(serializedKeyFormat []byte) (proto.Message, error) {
    return nil, errNotSupportedOnHPKE
}

func (p *hpkePublicKeyManager) NewKeyData(serializedKeyFormat []byte) (*tinkpb.KeyData, error) {
    return nil, errNotSupportedOnHPKE
}
