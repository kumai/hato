#!/bin/sh
# Regenerate deliberately public, test-only keys. Never use these keys outside tests.
# Run from any directory with a JDK keytool on PATH, or set KEYTOOL to its absolute path.
# Only PKCS12 files are kept; any PEM files needed by a broker are exported at runtime.
set -eu
umask 077

keytool_bin=${KEYTOOL:-keytool}
resource_dir=$(CDPATH= cd -- "$(dirname -- "$0")/../src/main/resources" && pwd)/tls
scratch_dir=$(mktemp -d)
trap 'rm -rf "$scratch_dir"' EXIT HUP INT TERM
password=mqtj-test-only
start_date='2020/01/01 00:00:00'
validity=36525

# The CA's private key never leaves the scratch directory: tests only need its certificate.
"$keytool_bin" -genkeypair -alias ca -dname 'CN=MQTJ Test CA, O=MQTJ Tests' \
    -keyalg RSA -keysize 2048 -sigalg SHA256withRSA \
    -startdate "$start_date" -validity "$validity" \
    -ext bc:critical=ca:true,pathlen:0 -ext ku:critical=keyCertSign,cRLSign \
    -storetype PKCS12 -keystore "$scratch_dir/ca-key.p12" -storepass "$password"
"$keytool_bin" -exportcert -alias ca -keystore "$scratch_dir/ca-key.p12" \
    -storepass "$password" -file "$scratch_dir/ca.crt"
"$keytool_bin" -importcert -noprompt -alias ca -file "$scratch_dir/ca.crt" \
    -storetype PKCS12 -keystore "$scratch_dir/ca.p12" -storepass "$password"

for identity in server client; do
    case "$identity" in
        server) purpose=serverAuth ;;
        client) purpose=clientAuth ;;
    esac
    "$keytool_bin" -genkeypair -alias "$identity" \
        -dname "CN=MQTJ Test $identity, O=MQTJ Tests" \
        -keyalg RSA -keysize 2048 -sigalg SHA256withRSA \
        -startdate "$start_date" -validity "$validity" \
        -storetype PKCS12 -keystore "$scratch_dir/$identity.p12" -storepass "$password"
    "$keytool_bin" -certreq -alias "$identity" -keystore "$scratch_dir/$identity.p12" \
        -storepass "$password" -file "$scratch_dir/$identity.csr"
    "$keytool_bin" -gencert -alias ca -keystore "$scratch_dir/ca-key.p12" \
        -storepass "$password" -infile "$scratch_dir/$identity.csr" \
        -outfile "$scratch_dir/$identity.crt" -sigalg SHA256withRSA \
        -startdate "$start_date" -validity "$validity" \
        -ext bc:critical=ca:false -ext ku:critical=digitalSignature,keyEncipherment \
        -ext "eku=$purpose" -ext san=dns:localhost,dns:test.mqtj.invalid
    "$keytool_bin" -importcert -noprompt -alias ca \
        -keystore "$scratch_dir/$identity.p12" -storepass "$password" \
        -file "$scratch_dir/ca.crt"
    "$keytool_bin" -importcert -alias "$identity" \
        -keystore "$scratch_dir/$identity.p12" -storepass "$password" \
        -file "$scratch_dir/$identity.crt"
done

mkdir -p "$resource_dir"
cp "$scratch_dir/ca.p12" "$scratch_dir/server.p12" "$scratch_dir/client.p12" "$resource_dir/"
chmod 644 "$resource_dir/ca.p12" "$resource_dir/server.p12" "$resource_dir/client.p12"
