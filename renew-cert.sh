#!/usr/bin/bash
# Renew the robot certificate from the Kerberos keytab.
set -e

# This script is designed to be run by cron.
# Set up the same environemnt variables as for a login shell.
source "$HOME/.bashrc"

X509_USER_CERT="$HOME/.globus/usercert.pem"
X509_USER_KEY="$HOME/.globus/userkey.pem"
KERBEROS_KEYTAB="${HOME}/.globus/krb5.keytab"
KERBEROS_PRINCIPAL="$(klist -k "${KERBEROS_KEYTAB}" | tail -n 1 | sed 's/.*\s//')"
kinit "${KERBEROS_PRINCIPAL}" -k -t "${KERBEROS_KEYTAB}"
ecp-get-cert -k > /dev/null
GRID_PROXY_PATH="$(ecp-cert-info -path)"
cp "${GRID_PROXY_PATH}" "${X509_USER_CERT}"
cp "${GRID_PROXY_PATH}" "${X509_USER_KEY}"
