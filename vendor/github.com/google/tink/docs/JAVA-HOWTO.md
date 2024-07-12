# Tink for Java HOW-TO

The following subsections present instructions and/or Java-snippets for some
common tasks in [Tink](https://github.com/google/tink).

If you want to contribute, please read [Java hacking guide](JAVA-HACKING.md).

## Installation

Tink for Java comes in two flavors:

-   The main flavor.
-   The Android flavor that is optimized for Android.

Tink can be installed with Maven or Gradle. The Maven group ID is
`com.google.crypto.tink`, and the artifact ID is `tink`.

The most recent release is
[1.2.2](https://github.com/google/tink/releases/tag/v1.2.2), released
2019-01-24.

Java developers can add Tink using Maven:

```xml
<dependency>
  <groupId>com.google.crypto.tink</groupId>
  <artifactId>tink</artifactId>
  <version>1.2.2</version>
</dependency>
```

Android developers can add Tink using Gradle:

```
dependencies {
  compile 'com.google.crypto.tink:tink-android:1.2.2'
}
```

### Snapshots

Snapshots of Tink built from the master branch are available through Maven using
version `HEAD-SNAPSHOT`.

To add a dependency using Maven:

```xml
<repositories>
<repository>
  <id>sonatype-snapshots</id>
  <name>sonatype-snapshots</name>
  <url>https://oss.sonatype.org/content/repositories/snapshots/</url>
  <snapshots>
    <enabled>true</enabled>
    <updatePolicy>always</updatePolicy>
  </snapshots>
  <releases>
    <updatePolicy>always</updatePolicy>
  </releases>
</repository>
</repositories>

<dependency>
  <groupId>com.google.crypto.tink</groupId>
  <artifactId>tink</artifactId>
  <version>HEAD-SNAPSHOT</version>
</dependency>
```

To add a dependency using Gradle:

```
repositories {
    maven { url "https://oss.sonatype.org/content/repositories/snapshots" }
}

dependencies {
  compile 'com.google.crypto.tink:tink-android:HEAD-SNAPSHOT'
}
```

## API docs

*   Java:
    *   [1.2.1](https://google.github.com/tink/javadoc/tink/1.2.1)
    *   [HEAD-SNAPSHOT](https://google.github.com/tink/javadoc/tink/HEAD-SNAPSHOT)
*   Android:
    *   [1.2.1](https://google.github.com/tink/javadoc/tink-android/1.2.1)
    *   [HEAD-SNAPSHOT](https://google.github.com/tink/javadoc/tink-android/HEAD-SNAPSHOT)

## Important Warnings

Do not use APIs including fields and methods marked with the `@Alpha`
annotation. They can be modified in any way, or even removed, at any time. They
are in the package, but not for official, production release, but only for
testing.

Do not use APIs in the `com.google.crypto.tink.subtle`. While they're generally
safe to use, they're not meant for public consumption and can be modified in any
way, or even removed, at any time.

## Initializing Tink

Tink provides customizable initialization, which allows for choosing specific
implementations (identified by _key types_) of desired primitives. This
initialization happens via _registration_ of the implementations.

For example, if you want to use all implementations of all primitives in Tink,
the initialization would look as follows:

```java
    import com.google.crypto.tink.config.TinkConfig;

    TinkConfig.register();
```

To use only implementations of the AEAD primitive:

```java
    import com.google.crypto.tink.aead.AeadConfig;

    AeadConfig.register();
```

For custom initialization the registration proceeds directly via
`Registry`-class:

```java
    import com.google.crypto.tink.Registry;
    import my.custom.package.aead.MyAeadKeyManager;

    // Register a custom implementation of AEAD.
    Registry.registerKeyManager(new MyAeadKeyManager());

```

## Generating New Key(set)s

Each `KeyManager`-implementation provides `newKey(..)`-methods that generate new
keys of the corresponding key type. However to avoid accidental leakage of
sensitive key material you should be careful with mixing key(set) generation
with key(set) usage in code. To support the separation between these activities
Tink package provides a command-line tool called [Tinkey](TINKEY.md), which can
be used for common key management tasks.

Still, if there is a need to generate a KeysetHandle with fresh key material
directly in Java code, you can use
[`KeysetHandle`](https://github.com/google/tink/blob/master/java/src/main/java/com/google/crypto/tink/KeysetHandle.java).
For example, you can generate a keyset containing a randomly generated
AES128-GCM key as follows.

```java
    import com.google.crypto.tink.KeysetHandle;
    import com.google.crypto.tink.aead.AeadKeyTemplates;

    KeyTemplate keyTemplate = AeadKeyTemplates.AES128_GCM;
    KeysetHandle keysetHandle = KeysetHandle.generateNew(keyTemplate);
```

Recommended key templates for MAC, digital signature and hybrid encryption can
be found in
[MacKeyTemplates](https://github.com/google/tink/blob/master/java/src/main/java/com/google/crypto/tink/mac/MacKeyTemplates.java),
[SignatureKeyTemplates](https://github.com/google/tink/blob/master/java/src/main/java/com/google/crypto/tink/signature/SignatureKeyTemplates.java)
and
[HybridKeyTemplates](https://github.com/google/tink/blob/master/java/src/main/java/com/google/crypto/tink/hybrid/HybridKeyTemplates.java),
respectively.

## Storing Keysets

After generating key material, you might want to persist it to a storage system,
e.g., writing to a file:

```java
    import com.google.crypto.tink.CleartextKeysetHandle;
    import com.google.crypto.tink.KeysetHandle;
    import com.google.crypto.tink.aead.AeadKeyTemplates;
    import com.google.crypto.tink.JsonKeysetWriter;
    import java.io.File;

    // Generate the key material...
    KeysetHandle keysetHandle = KeysetHandle.generateNew(
        AeadKeyTemplates.AES_128_GCM);

    // and write it to a file.
    String keysetFilename = "my_keyset.json";
    CleartextKeysetHandle.write(keysetHandle, JsonKeysetWriter.withFile(
        new File(keysetFilename));
```

Storing cleartext keysets on disk is not recommended. Tink supports encrypting
keysets with master keys stored in a remote [key management
systems](KEY-MANAGEMENT.md).

For example, you can encrypt the key material with a Google Cloud KMS key at
`gcp-kms:/projects/tink-examples/locations/global/keyRings/foo/cryptoKeys/bar`
as follows:

```java
    import com.google.crypto.tink.JsonKeysetWriter;
    import com.google.crypto.tink.KeysetHandle;
    import com.google.crypto.tink.aead.AeadKeyTemplates;
    import com.google.crypto.tink.integration.gcpkms.GcpKmsClient;
    import java.io.File;

    // Generate the key material...
    KeysetHandle keysetHandle = KeysetHandle.generateNew(
        AeadKeyTemplates.AES_128_GCM);

    // and write it to a file...
    String keysetFilename = "my_keyset.json";
    // encrypted with the this key in GCP KMS
    String masterKeyUri = "gcp-kms://projects/tink-examples/locations/global/keyRings/foo/cryptoKeys/bar";
    keysetHandle.write(JsonKeysetWriter.withFile(new File(keysetFilename)),
        new GcpKmsClient().getAead(masterKeyUri));
```

## Loading Existing Keysets

To load encrypted keysets, you can use
[`KeysetHandle`](https://github.com/google/tink/blob/master/java/src/main/java/com/google/crypto/tink/KeysetHandle.java):

```java
    import com.google.crypto.tink.JsonKeysetReader;
    import com.google.crypto.tink.KeysetHandle;
    import com.google.crypto.tink.integration.awskms.AwsKmsClient;
    import java.io.File;

    String keysetFilename = "my_keyset.json";
    // The keyset is encrypted with the this key in AWS KMS.
    String masterKeyUri = "aws-kms://arn:aws:kms:us-east-1:007084425826:key/84a65985-f868-4bfc-83c2-366618acf147";
    KeysetHandle keysetHandle = KeysetHandle.read(
        JsonKeysetReader.withFile(new File(keysetFilename)),
        new AwsKmsClient().getAead(masterKeyUri));
```

To load cleartext keysets, use
[`CleartextKeysetHandle`](https://github.com/google/tink/blob/master/java/src/main/java/com/google/crypto/tink/CleartextKeysetHandle.java):

```java
    import com.google.crypto.tink.CleartextKeysetHandle;
    import com.google.crypto.tink.KeysetHandle;
    import java.io.File;

    String keysetFilename = "my_keyset.json";
    KeysetHandle keysetHandle = CleartextKeysetHandle.read(
        JsonKeysetReader.withFile(new File(keysetFilename)));
```

## Obtaining and Using Primitives

[_Primitives_](PRIMITIVES.md) represent cryptographic operations offered by
Tink, hence they form the core of Tink API. A primitive is just an interface
that specifies what operations are offered by the primitive. A primitive can
have multiple implementations, and user chooses a desired implementation by
using a key of corresponding type (see the [this
section](KEY-MANAGEMENT.md#key-keyset-and-keysethandle) for details).

The following table summarizes Java implementations of primitives that are
currently available or planned (the latter are listed in brackets).

| Primitive          | Implementations                                |
| ------------------ | ---------------------------------------------- |
| AEAD               | AES-EAX, AES-GCM, AES-CTR-HMAC, KMS Envelope,  |
:                    : CHACHA20-POLY1305                              :
| Streaming AEAD     | AES-GCM-HKDF-STREAMING, AES-CTR-HMAC-STREAMING |
| Deterministic AEAD | AES-SIV                                        |
| MAC                | HMAC-SHA2                                      |
| Digital Signatures | ECDSA over NIST curves, ED25519                |
| Hybrid Encryption  | ECIES with AEAD and HKDF, (NaCl CryptoBox)     |

Exact listings of primitives and their implementations available in a release _x.y.z_ of Tink
are given in a corresponding [`TinkConfig.TINK_x_y_z`](https://github.com/google/tink/blob/master/java/src/main/java/com/google/crypto/tink/config/TinkConfig.java)-variable.

Tink user accesses implementations of a primitive via a factory that corresponds
to the primitive: AEAD via `AeadFactory`, MAC via `MacFactory`, etc. where each
factory offers corresponding `getPrimitive(...)` methods.

### Symmetric Key Encryption

Here is how you can obtain and use an [AEAD (Authenticated Encryption with
Associated Data](PRIMITIVES.md#authenticated-encryption-with-associated-data)
primitive to encrypt or decrypt data:

```java
    import com.google.crypto.tink.Aead;
    import com.google.crypto.tink.KeysetHandle;
    import com.google.crypto.tink.aead.AeadFactory;
    import com.google.crypto.tink.aead.AeadKeyTemplates;

    // 1. Generate the key material.
    KeysetHandle keysetHandle = KeysetHandle.generateNew(
        AeadKeyTemplates.AES128_GCM);

    // 2. Get the primitive.
    Aead aead = AeadFactory.getPrimitive(keysetHandle);

    // 3. Use the primitive to encrypt a plaintext,
    byte[] ciphertext = aead.encrypt(plaintext, aad);

    // ... or to decrypt a ciphertext.
    byte[] decrypted = aead.decrypt(ciphertext, aad);
```

### Deterministic Symmetric Key Encryption

Here is how you can obtain and use an [DeterministicAEAD (Deterministic
Authenticated Encryption with Associated
Data](PRIMITIVES.md#deterministic-authenticated-encryption-with-associated-data)
primitive to encrypt or decrypt data:

```java
    import com.google.crypto.tink.DeterministicAead;
    import com.google.crypto.tink.KeysetHandle;
    import com.google.crypto.tink.daead.DeterministicAeadFactory;
    import com.google.crypto.tink.daead.DeterministicAeadKeyTemplates;

    // 1. Generate the key material.
    KeysetHandle keysetHandle = KeysetHandle.generateNew(
        DeterministicAeadKeyTemplates.AES256_SIV);

    // 2. Get the primitive.
    DeterministicAead daead =
        DeterministicAeadFactory.getPrimitive(keysetHandle);

    // 3. Use the primitive to deterministically encrypt a plaintext,
    byte[] ciphertext = daead.encryptDeterministically(plaintext, aad);

    // ... or to deterministically decrypt a ciphertext.
    byte[] decrypted = daead.decryptDeterministically(ciphertext, aad);
```

### Symmetric Key Encryption of Streaming Data

Here is how you can obtain and use an [Streaming AEAD (Streaming Authenticated Encryption with
Associated Data)](PRIMITIVES.md#streaming-authenticated-encryption-with-associated-data) primitive
to encrypt or decrypt data streams:

```java
    import com.google.crypto.tink.StreamingAead;
    import com.google.crypto.tink.KeysetHandle;
    import com.google.crypto.tink.streamingaead.StreamingAeadFactory;
    import com.google.crypto.tink.streamingaead.StreamingAeadKeyTemplates;
    import java.nio.ByteBuffer
    import java.nio.channels.FileChannel;
    import java.nio.channels.SeekableByteChannel;
    import java.nio.channels.WritableByteChannel;

    // 1. Generate the key material.
    KeysetHandle keysetHandle = KeysetHandle.generateNew(
        StreamingAeadKeyTemplates.AES128_CTR_HMAC_SHA256_4KB);

    // 2. Get the primitive.
    StreamingAead streamingAead = StreamingAeadFactory.getPrimitive(keysetHandle);

    // 3. Use the primitive to encrypt some data and write the ciphertext to a file,
    FileChannel ciphertextDestination = new FileOutputStream(ciphertextFileName).getChannel();
    byte[] aad = ...
    WritableByteChannel encryptingChannel =
        streamingAead.newEncryptingChannel(ciphertextDestination, aad);
    ByteBuffer buffer = ByteBuffer.allocate(chunkSize);
    while ( bufferContainsDataToEncrypt ) {
      int r = encryptingChannel.write(buffer);
      // Try to get into buffer more data for encryption.
    }
    // Complete the encryption (process the remaining plaintext, if any, and close the channel).
    encryptingChannel.close();

    // ... or to decrypt an existing ciphertext stream.
    FileChannel ciphertextSource = new FileInputStream(ciphertextFileName).getChannel();
    byte[] aad = ...
    ReadableByteChannel decryptingChannel = s.newDecryptingChannel(ciphertextSource, aad);
    ByteBuffer buffer = ByteBuffer.allocate(chunkSize);
    do {
      buffer.clear();
      int cnt = decryptingChannel.read(buffer);
      if (cnt > 0) {
        // Process cnt bytes of plaintext.
      } else if (read == -1) {
        // End of plaintext detected.
        break;
      } else if (read == 0) {
        // No ciphertext is available at the moment.
      }
   }
```

### Message Authentication Code

The following snippets shows how to compute or verify a [MAC (Message
Authentication Code)](PRIMITIVES.md#message-authentication-code):

```java
    import com.google.crypto.tink.KeysetHandle;
    import com.google.crypto.tink.Mac;
    import com.google.crypto.tink.mac.MacFactory;
    import com.google.crypto.tink.mac.MacKeyTemplates;

    // 1. Generate the key material.
    KeysetHandle keysetHandle = KeysetHandle.generateNew(
        MacKeyTemplates.HMAC_SHA256_128BITTAG);

    // 2. Get the primitive.
    Mac mac = MacFactory.getPrimitive(keysetHandle);

    // 3. Use the primitive to compute a tag,
    byte[] tag = mac.computeMac(data);

    // ... or to verify a tag.
    mac.verifyMac(tag, data);
```

### Digitial Signatures

Here is an example of how to sign or verify a [digital
signature](PRIMITIVES.md#digital-signatures):

```java
    import com.google.crypto.tink.KeysetHandle;
    import com.google.crypto.tink.PublicKeySign;
    import com.google.crypto.tink.PublicKeyVerify;
    import com.google.crypto.tink.signature.PublicKeySignFactory;
    import com.google.crypto.tink.signature.PublicKeyVerifyFactory;
    import com.google.crypto.tink.signature.SignatureKeyTemplates;

    // SIGNING

    // 1. Generate the private key material.
    KeysetHandle privateKeysetHandle = KeysetHandle.generateNew(
        SignatureKeyTemplates.ECDSA_P256);

    // 2. Get the primitive.
    PublicKeySign signer = PublicKeySignFactory.getPrimitive(
        privateKeysetHandle);

    // 3. Use the primitive to sign.
    byte[] signature = signer.sign(data);

    // VERIFYING

    // 1. Obtain a handle for the public key material.
    KeysetHandle publicKeysetHandle =
        privateKeysetHandle.getPublicKeysetHandle();

    // 2. Get the primitive.
    PublicKeyVerify verifier = PublicKeyVerifyFactory.getPrimitive(
        publicKeysetHandle);

    // 4. Use the primitive to verify.
    verifier.verify(signature, data);
```

### Hybrid Encryption

To encrypt or decrypt using [a combination of public key encryption and
symmetric key encryption](PRIMITIVES.md#hybrid-encryption) one can
use the following:

```java
    import com.google.crypto.tink.HybridDecrypt;
    import com.google.crypto.tink.HybridEncrypt;
    import com.google.crypto.tink.hybrid.HybridDecryptFactory;
    import com.google.crypto.tink.hybrid.HybridEncryptFactory;
    import com.google.crypto.tink.hybrid.HybridKeyTemplates;
    import com.google.crypto.tink.KeysetHandle;

    // 1. Generate the private key material.
    KeysetHandle privateKeysetHandle = KeysetHandle.generateNew(
        HybridKeyTemplates.ECIES_P256_HKDF_HMAC_SHA256_AES128_GCM);

    // Obtain the public key material.
    KeysetHandle publicKeysetHandle =
        privateKeysetHandle.getPublicKeysetHandle();

    // ENCRYPTING

    // 2. Get the primitive.
    HybridEncrypt hybridEncrypt = HybridEncryptFactory.getPrimitive(
        publicKeysetHandle);

    // 3. Use the primitive.
    byte[] ciphertext = hybridEncrypt.encrypt(plaintext, contextInfo);

    // DECRYPTING

    // 2. Get the primitive.
    HybridDecrypt hybridDecrypt = HybridDecryptFactory.getPrimitive(
        privateKeysetHandle);

    // 3. Use the primitive.
    byte[] plaintext = hybridDecrypt.decrypt(ciphertext, contextInfo);
```

### Envelope Encryption

Via the AEAD interface, Tink supports
[envelope](http://docs.aws.amazon.com/kms/latest/developerguide/workflow.html)
[encryption](https://cloud.google.com/kms/docs/data-encryption-keys) (a.k.a. KMS
Envelope) which is getting popular with Cloud users.

In this mode, Cloud users first create a key encryption key (KEK) in a Key
Management System (KMS) such as AWS KMS or Google Cloud KMS. To encrypt some
data, users then generate locally a data encryption key (DEK), encrypt data with
the DEK, ask the KMS to encrypt the DEK with the KEK, and stores the encrypted
DEK with the encrypted data. At a later point Cloud users can retrieve encrypted
data and the DEK, ask the KMS to decrypt DEK, and use the decrypted DEK to
decrypt the data.

For example, you can perform envelope encryption with a Google Cloud KMS key at
`gcp-kms://projects/tink-examples/locations/global/keyRings/foo/cryptoKeys/bar`
using the credentials in `credentials.json` as follows:

```java
    import com.google.crypto.tink.Aead;
    import com.google.crypto.tink.KeysetHandle;
    import com.google.crypto.tink.KmsClients;
    import com.google.crypto.tink.aead.AeadFactory;
    import com.google.crypto.tink.aead.AeadKeyTemplates;
    import com.google.crypto.tink.integration.gcpkms.GcpKmsClient;

    // 1. Generate the key material.
    String kmsKeyUri =
        "gcp-kms://projects/tink-examples/locations/global/keyRings/foo/cryptoKeys/bar";
    KeysetHandle keysetHandle = KeysetHandle.generateNew(
        AeadKeyTemplates.createKmsEnvelopeAeadKeyTemplate(kmsKeyUri, AeadKeyTemplates.AES128_GCM));

    // 2. Register the KMS client.
    KmsClients.add(new GcpKmsClient()
        .withCredentials("credentials.json"));

    // 3. Get the primitive.
    Aead aead = AeadFactory.getPrimitive(keysetHandle);

    // 4. Use the primitive.
    byte[] ciphertext = aead.encrypt(plaintext, aad);
```

## Key Rotation

The support for key rotation in Tink is provided via
[`KeysetManager`](https://github.com/google/tink/blob/master/java/src/main/java/com/google/crypto/tink/KeysetManager.java)-class.

You have to provide a `KeysetHandle`-object that contains the keyset that should
be rotated, and a specification of the new key via a
[`KeyTemplate`](https://github.com/google/tink/blob/master/proto/tink.proto#L50)-message.

```java
    import com.google.crypto.tink.KeysetHandle;
    import com.google.crypto.tink.KeysetManager;
    import com.google.crypto.tink.proto.KeyTemplate;

    KeysetHandle keysetHandle = ...;   // existing keyset
    KeyTemplate keyTemplate = ...;     // template for the new key

    KeysetHandle rotatedKeysetHandle = KeysetManager
        .withKeysetHandle(keysetHandle)
        .rotate(keyTemplate)
        .getKeysetHandle();
```

Some common specifications are available as pre-generated templates
in [examples/keytemplates](https://github.com/google/tink/tree/master/examples/keytemplates)-folder,
and can be accessed via `...KeyTemplates.java` classes of the respective
primitives.  After a successful rotation the resulting keyset contains a new key
generated according to the specification in `keyTemplate`, and the new key
becomes the _primary key_ of the keyset.  For the rotation to succeed the
`Registry` must contain a key manager for the key type specified in
`keyTemplate`.

Alternatively, you can use [Tinkey](TINKEY.md) to rotate or manage a keyset.

## Custom Implementation of a Primitive

**NOTE**: The usage of **custom key managers should be enjoyed
responsibly**: we (i.e. Tink developers) have no way checking or enforcing that
a custom implementation satisfies security properties of the corresponding
primitive interface, so it is up to the implementer and the user of the custom
implementation ensure the required properties.

**TIP** For a working example, please check out the
[AES-CBC-HMAC](https://github.com/thaidn/tink-examples/blob/master/timestamper/src/main/java/com/timestamper/AesCbcHmacKeyManager.java)
implementation of the AEAD primitive in the
[timestamper](https://github.com/thaidn/tink-examples/tree/master/timestamper)
example.

The main cryptographic operations offered by Tink are accessible via so-called
_primitives_, which essentially are interfaces that represent corresponding
cryptographic functionalities. While Tink comes with several standard
implementations of common primitives, it allows also for adding custom
implementations of primitives. Such implementations allow for seamless
integration of Tink with custom third-party cryptographic schemes or hardware
modules, and in combination with [key rotation](#key-rotation)-features enable
painless migration between cryptographic schemes.

To create a custom implementation of a primitive proceed as follows:

1.  Determine for which _primitive_ a custom implementation is needed.
2.  Define protocol buffers that hold key material and parameters for the custom
    cryptographic scheme; the name of the key protocol buffer (a.k.a. type URL)
    determines the _key type_ for the custom implementation.
3.  Implement
    [`KeyManager`](https://github.com/google/tink/blob/master/java/src/main/java/com/google/crypto/tink/KeyManager.java)
    interface for the _primitive_ from step #1 and the _key type_ from step #2.

To use a custom implementation of a primitive in an application, register with
the [`Registry`](https://github.com/google/tink/blob/master/java/src/main/java/com/google/crypto/tink/Registry.java)
the custom `KeyManager`-implementation (from step #3 above) for the custom key
type (from step #2 above):

```java
    Registry.registerKeyManager(keyManager);
```

Afterwards the implementation will be accessed automatically by the `Factory`
corresponding to the primitive (when keys of the specific key type are in use),
or can be retrieved directly via `Registry.getKeyManager(keyType)`.

When defining the protocol buffers for the key material and parameters (step #2
above), you should provide definitions of three messages:

 * `...Params`: parameters of an instantiation of the primitive,
   needed when a key is being used.
 * `...Key`: the actual key proto, contains the key material and the
   corresponding `...Params`-proto.
 * `...KeyFormat`: parameters needed to generate a new key.

Here are a few conventions/recommendations wrt. defining these messages
(see [tink.proto](https://github.com/google/tink/blob/master/proto/tink.proto)
and definitions of [existing key types](https://github.com/google/tink/blob/master/proto/)
for details):

 * `...Key` should contain a version field (a monotonic counter, `uint32 version;`),
   which identifies the version of implementation that can work with this key.
 * `...Params` should be a field of `...Key`, as by definition `...Params`
   contains parameters needed when the key is being used.
 * `...Params` should be also a field of `...KeyFormat`, so that given `...KeyFormat`
   one has all information it needs to generate a new `...Key` message.

Alternatively, depending on the use case requirements, you can skip step #2
entirely and re-use for the key material an existing protocol buffer messages.
In such a case you should not configure the Registry via the `Config`-class, but
rather register the needed `KeyManager`-instances manually.

For a concrete example, let's assume that we'd like a custom implementation of
[`Aead`](https://github.com/google/tink/blob/master/java/src/main/java/com/google/crypto/tink/Aead.java)-primitive
(step #1). We define then three protocol buffer messages (step #2):

 * `MyCustomAeadParams`: holds parameters needed for the use of the key material.
 * `MyCustomAeadKey`: holds the actual key material and parameters needed for its use.
 * `MyCustomAeadKeyFormat`: holds parameters needed for generation of a new `MyCustomAeadKey`-key.

```protocol-buffer
    syntax = "proto3";
    package mycompany.mypackage;

    message MyCustomAeadParams {
      uint32 iv_size = 1;     // size of initialization vector in bytes
    }

    message MyCustomAeadKeyFormat {
      MyCustomAeadParams params = 1;
      uint32 key_size = 2;    // key size in bytes
    }

    // key_type: type.googleapis.com/mycompany.mypackage.MyCustomAeadKey
    message MyCustomAeadKey {
        uint32 version = 1;
        MyCustomAeadParams params = 2;
        bytes key_value = 3;  // the actual key material
    }
```

The corresponding _key type_ in Java is defined as

```java
    String keyType = "type.googleapis.com/mycompany.mypackage.MyCustomAeadKey";`
```

and the corresponding _key manager_ implements (step #3) the interface
[`KeyManager<Aead>`](https://github.com/google/tink/blob/master/java/src/main/java/com/google/crypto/tink/KeyManager.java)

```java
    class MyCustomAeadKeyManager implements KeyManager<Aead> {
      // ...
    }
```

After registering `MyCustomAeadKeyManager` with the Registry we can use it
via [`AeadFactory`](https://github.com/google/tink/blob/master/java/src/main/java/com/google/crypto/tink/aead/AeadFactory.java).
