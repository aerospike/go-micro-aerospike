#!/bin/sh
# Generates throwaway PKI material for the opt-in TLS fixture into docker-certs/.
# Adapted from the agent-squad-aerospike fixture. Output:
#   ca.crt/ca.key                  - test CA (signs server + client certs)
#   server.crt/server.key        - server cert, CN=aerospike-tls, SAN DNS:aerospike-tls,IP:127.0.0.1
#   client.crt/client.key        - client cert, CN=go-micro-tls-client (PKI user CN)
#   other-ca.crt/other-ca.key    - unrelated CA (untrusted-CA negative test)
#   untrusted-client.*/          - client cert signed by other-ca (mTLS negative test)
#   other-user.crt/.key          - CA-signed cert whose CN is not a provisioned
#                                  user (invalid-credentials negative test)
set -eu
umask 077

ROOT="${AEROSPIKE_TLS_CERT_DIR:-$(dirname "$0")/../docker-certs}"
mkdir -p "$ROOT/server"

mk() { openssl "$@"; }

# --- CA ---
mk req -x509 -newkey rsa:2048 -nodes -days 30 \
	-addext "basicConstraints=critical,CA:TRUE" \
	-addext "keyUsage=critical,keyCertSign" \
	-keyout "$ROOT/ca.key" -out "$ROOT/ca.crt" -subj "/CN=go-micro-test-ca"

# --- Server cert ---
mk req -newkey rsa:2048 -nodes \
	-keyout "$ROOT/server.key" -out "$ROOT/server.csr" -subj "/CN=aerospike-tls"
printf 'subjectAltName=DNS:aerospike-tls,IP:127.0.0.1\nextendedKeyUsage=serverAuth\nkeyUsage=digitalSignature,keyEncipherment\n' > "$ROOT/server.ext"
mk x509 -req -in "$ROOT/server.csr" -days 30 \
	-CA "$ROOT/ca.crt" -CAkey "$ROOT/ca.key" -CAcreateserial \
	-out "$ROOT/server.crt" -extfile "$ROOT/server.ext"

# --- Client cert (CN is the PKI username) ---
mk req -newkey rsa:2048 -nodes \
	-keyout "$ROOT/client.key" -out "$ROOT/client.csr" -subj "/CN=go-micro-tls-client"
printf 'basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature\nextendedKeyUsage=clientAuth\n' > "$ROOT/client.ext"
mk x509 -req -in "$ROOT/client.csr" -days 30 \
	-CA "$ROOT/ca.crt" -CAkey "$ROOT/ca.key" -CAcreateserial \
	-out "$ROOT/client.crt" -extfile "$ROOT/client.ext"

# --- Untrusted CA + client cert signed by it ---
mk req -x509 -newkey rsa:2048 -nodes -days 30 \
	-addext "basicConstraints=critical,CA:TRUE" \
	-addext "keyUsage=critical,keyCertSign" \
	-keyout "$ROOT/other-ca.key" -out "$ROOT/other-ca.crt" -subj "/CN=untrusted-ca"
mk req -newkey rsa:2048 -nodes \
	-keyout "$ROOT/untrusted-client.key" -out "$ROOT/untrusted-client.csr" -subj "/CN=untrusted-client"
mk x509 -req -in "$ROOT/untrusted-client.csr" -days 30 \
	-CA "$ROOT/other-ca.crt" -CAkey "$ROOT/other-ca.key" -CAcreateserial \
	-out "$ROOT/untrusted-client.crt" -extfile "$ROOT/client.ext"

# --- CA-signed client cert whose CN is not a provisioned user ---
mk req -newkey rsa:2048 -nodes \
	-keyout "$ROOT/other-user.key" -out "$ROOT/other-user.csr" -subj "/CN=not-a-provisioned-user"
mk x509 -req -in "$ROOT/other-user.csr" -days 30 \
	-CA "$ROOT/ca.crt" -CAkey "$ROOT/ca.key" -CAcreateserial \
	-out "$ROOT/other-user.crt" -extfile "$ROOT/client.ext"

cp "$ROOT/server.crt" "$ROOT/server.key" "$ROOT/ca.crt" "$ROOT/server/"
rm -f "$ROOT"/*.csr "$ROOT"/*.srl "$ROOT"/*.ext
chmod 644 "$ROOT"/*.crt "$ROOT"/server/*.crt
echo "TLS test certs ready in $ROOT"
