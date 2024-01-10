#!/usr/bin/env bash

mypass="pass123"

echo Generate server key:
openssl genrsa -passout pass:$mypass -des3 -out server.key 4096

echo Generate server signing request:
openssl req -passin pass:$mypass -new -key server.key -out server1.csr -subj "/C=RU/L=SaintPetersburg/O=Yandex/OU=Infrastructure/CN=localhost"
openssl req -passin pass:$mypass -new -key server.key -out server2.csr -subj "/C=RU/L=SaintPetersburg/O=Yandex/OU=Infrastructure/CN=$(hostname --fqdn)"

echo Self-sign server certificate:
openssl x509 -req -passin pass:$mypass -days 36500 -in server1.csr -signkey server.key -set_serial 01 -out server.crt -extfile <(printf "subjectAltName=DNS:localhost")
openssl x509 -req -passin pass:$mypass -days 36500 -in server2.csr -signkey server.key -set_serial 01 -out server_fallback.crt -extfile <(printf "subjectAltName=DNS:$(hostname --fqdn)")

echo Remove passphrase from server key:
openssl rsa -passin pass:$mypass -in server.key -out server.key

rm server1.csr server2.csr
