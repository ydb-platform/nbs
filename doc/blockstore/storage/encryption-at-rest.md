# Encryption at rest (NRD/Mirrored)

## General information
The data encryption key (DEK) is stored in encrypted form in the volume metadata ([EncryptionDesc](https://github.com/ydb-platform/nbs/blob/7e02f27e5ff0473ce899ed037d35897de43d09db/contrib/ydb/core/protos/blockstore_config.proto#L117)). For DEK generation and encryption, the root key management service (RootKMS) is used. A key encryption key (KEK) that is used for encryption and decryption of DEKs is stored in RootKMS and is not transmitted in plain text - requests to RootKMS use only the identifier of the master key. If the master key is compromised, a new one is generated and then all DEKs must be re-encrypted (and updated in the volume metadata) using the new master key.

## Creating a volume
NBS requests a new DEK from RootKMS, sending the KEK identifier. The received encrypted DEK is stored in the volume metadata.

```mermaid
sequenceDiagram
    participant NBS
    participant RootKMS
    
    NBS->>+RootKMS: generate DEK (KEK id)
    RootKMS-->>-NBS: encrypted DEK
    NBS->>NBS: store DEK in VolumeConfig
```

## Mounting a volume
NBS requests the decrypted DEK from RootKMS, sending the encrypted DEK and the KEK ID. The received DEK is used for the encryption/decryption of IO request data. To handle IO requests, the [TEncryptionClient](https://github.com/ydb-platform/nbs/blob/7e02f27e5ff0473ce899ed037d35897de43d09db/cloud/blockstore/libs/encryption/encryption_client.cpp#L138) is used.

```mermaid
sequenceDiagram
    participant NBS
    participant RootKMS

    NBS->>NBS: get encrypted DEK from VolumeConfig
    NBS->>+RootKMS: decrypt DEK (KEK id, encrypted DEK)
    RootKMS-->>-NBS: DEK
    NBS->>NBS: create TEncryptionClient (DEK)
```
