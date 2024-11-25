#!/bin/bash

VALIDITY_DAYS=3650 # 10 years
COUNTRY="XX"
STATE="State"
LOCALITY="City"
ORGANIZATION="Test Organization"
ORG_UNIT="IT"
CA_CN="Test Root CA"
SERVER_CN="localhost"
CLIENT_CN="test-client"

generate_key_and_csr() {
    local PREFIX=$1
    local CN=$2

    openssl genrsa -out $PREFIX.key 4096

    openssl req -new \
        -key $PREFIX.key \
        -out $PREFIX.csr \
        -subj "/C=$COUNTRY/ST=$STATE/L=$LOCALITY/O=$ORGANIZATION/OU=$ORG_UNIT/CN=$CN"
}

# CA

openssl genrsa -out ca.key 4096

openssl req -new -x509 -sha256 \
    -days $VALIDITY_DAYS \
    -key ca.key \
    -out ca.crt \
    -subj "/C=$COUNTRY/ST=$STATE/L=$LOCALITY/O=$ORGANIZATION/OU=$ORG_UNIT/CN=$CA_CN"

# server

generate_key_and_csr "server" $SERVER_CN

openssl x509 -req -sha256 \
    -days $VALIDITY_DAYS \
    -in server.csr \
    -CA ca.crt \
    -CAkey ca.key \
    -CAcreateserial \
    -out server.crt \
    -extfile <(printf "subjectAltName=DNS:localhost,DNS:127.0.0.1\nbasicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth")

# client

generate_key_and_csr "client" $CLIENT_CN

openssl x509 -req -sha256 \
    -days $VALIDITY_DAYS \
    -in client.csr \
    -CA ca.crt \
    -CAkey ca.key \
    -CAcreateserial \
    -out client.crt \
    -extfile <(printf "basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=clientAuth")

# generate RSA key for nbs

openssl genrsa -out nbs.key 4096

# cleanup

rm *.csr

# check

ls -l *.crt *.key
