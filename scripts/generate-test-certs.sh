#!/usr/bin/env bash
# Generates a throw-away PKI for running NiFi with mutual TLS locally:
#   certs/ca.crt               - test CA (trust this in the .NET client)
#   certs/keystore.p12         - NiFi server key + certificate (SAN: localhost, nifi, 127.0.0.1)
#   certs/truststore.p12       - NiFi truststore containing the test CA
#   certs/client.p12           - client certificate + key for NifiKit (PKCS#12)
#   certs/client.crt/.key      - the same client certificate + key as PEM
#
# The client identity is "CN=nifikit-client", which docker-compose.yml configures
# as NiFi's initial admin. These certificates are for local testing only.
set -euo pipefail

OUT_DIR="${1:-$(cd "$(dirname "$0")/.." && pwd)/certs}"
PASSWORD="${NIFIKIT_CERT_PASSWORD:-nifikit-test-password}"
DAYS=825

if ! openssl pkcs12 -help 2>&1 | grep -q -- '-jdktrust'; then
  echo "OpenSSL 3.2 or newer is required (for 'pkcs12 -jdktrust'); found: $(openssl version)" >&2
  exit 1
fi

mkdir -p "$OUT_DIR"
cd "$OUT_DIR"

if [ -f ca.crt ] && [ "${FORCE:-0}" != "1" ]; then
  echo "Certificates already exist in $OUT_DIR (set FORCE=1 to regenerate)."
  exit 0
fi

echo "Generating test CA"
openssl req -x509 -newkey rsa:3072 -nodes -sha256 -days "$DAYS" \
  -keyout ca.key -out ca.crt -subj "/CN=NifiKit Test CA" \
  -addext "basicConstraints=critical,CA:TRUE" \
  -addext "keyUsage=critical,keyCertSign,cRLSign" 2>/dev/null

issue() {
  local name="$1" subject="$2" ext="$3"
  openssl req -newkey rsa:2048 -nodes -sha256 -keyout "$name.key" \
    -out "$name.csr" -subj "$subject" 2>/dev/null
  printf '%b' "$ext" > "$name.ext"
  openssl x509 -req -in "$name.csr" -CA ca.crt -CAkey ca.key -CAcreateserial \
    -days "$DAYS" -sha256 -extfile "$name.ext" -out "$name.crt" 2>/dev/null
  rm -f "$name.csr" "$name.ext"
}

echo "Generating NiFi server certificate"
issue server "/CN=localhost" \
  "basicConstraints=CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth,clientAuth\nsubjectAltName=DNS:localhost,DNS:nifi,IP:127.0.0.1\n"

echo "Generating client certificate (CN=nifikit-client)"
issue client "/CN=nifikit-client" \
  "basicConstraints=CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=clientAuth\n"

echo "Building NiFi keystore and truststore (PKCS#12)"
openssl pkcs12 -export -name nifi-key -in server.crt -inkey server.key \
  -certfile ca.crt -out keystore.p12 -passout "pass:$PASSWORD"
# -jdktrust marks the CA as a trusted certificate entry so Java/NiFi loads it
openssl pkcs12 -export -nokeys -in ca.crt -jdktrust anyExtendedKeyUsage \
  -out truststore.p12 -passout "pass:$PASSWORD"

echo "Building client PKCS#12"
openssl pkcs12 -export -name nifikit-client -in client.crt -inkey client.key \
  -certfile ca.crt -out client.p12 -passout "pass:$PASSWORD"

# NiFi runs as uid 1000 inside the container and must be able to read the stores
chmod 644 keystore.p12 truststore.p12 ca.crt client.crt client.p12
chmod 600 ca.key server.key client.key

echo "Done. Files written to $OUT_DIR (password: $PASSWORD)"
