#!/usr/bin/bash
# Renew the robot certificate from the Kerberos keytab.
set -e

# This script is designed to be run by cron.
# Set up the same environemnt variables as for a login shell.
source "$HOME/.bashrc"

kinit_keytab() {
    local principal="$(klist -k "$1" | tail -n 1 | sed 's/.*\s//')"
    kinit "${principal}" -k -t "$1"
}

X509_USER_CERT="$HOME/.globus/usercert.pem"
X509_USER_KEY="$HOME/.globus/userkey.pem"
kinit_keytab "${HOME}/.globus/krb5.keytab"
ecp-get-cert -k > /dev/null
GRID_PROXY_PATH="$(ecp-cert-info -path)"
cp "${GRID_PROXY_PATH}" "${X509_USER_CERT}"
cp "${GRID_PROXY_PATH}" "${X509_USER_KEY}"

kinit_keytab "${HOME}/read-cvmfs.keytab"
htgettoken -v -a vault.ligo.org -i igwn -r read-cvmfs-${USER} --scopes=read:/virgo --credkey=read-cvmfs-${USER}/robot/${USER}.ligo.caltech.edu --nooidc >> ${HOME}/htgettoken_output.txt
