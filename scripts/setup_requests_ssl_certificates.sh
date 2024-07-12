#!/bin/bash

set -euo pipefail

# Create directory to store SSL certificates in
ssl_dir="$HOME/Library/SSL"
if [ ! -d "$ssl_dir" ]; then
	mkdir "$ssl_dir"
fi

# Fetch certificates from KeyChain in PEM format
certs_pem="$ssl_dir/ca-certificates.pem"
security find-certificate -a -p /System/Library/Keychains/SystemRootCertificates.keychain >"$certs_pem"
security find-certificate -a -p -c "Genome Research Ltd" >>"$certs_pem"

echo "Add the following line to your ~/.zprofile file:"
echo "export REQUESTS_CA_BUNDLE=\"\$HOME/Library/SSL/ca-certificates.pem\""
