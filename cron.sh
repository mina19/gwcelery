#!/usr/bin/bash
# Perform hourly maintenance activities.

# Stop on error.
set -e

# This script is designed to be run by cron.
# Set up the same environemnt variables as for a login shell.
source "$HOME/.bashrc"

kinit_keytab() {
    local principal="$(klist -k "$1" | tail -n 1 | sed 's/.*\s//')"
    kinit "${principal}" -k -t "$1"
}

# Renew GraceDB credentials.
X509_USER_CERT="$HOME/.globus/usercert.pem"
X509_USER_KEY="$HOME/.globus/userkey.pem"
kinit_keytab "${HOME}/.globus/krb5.keytab"
ecp-get-cert -k > /dev/null
GRID_PROXY_PATH="$(ecp-cert-info -path)"
cp "${GRID_PROXY_PATH}" "${X509_USER_CERT}"
cp "${GRID_PROXY_PATH}" "${X509_USER_KEY}"

# Renew CVMFS credentials.
kinit_keytab "${HOME}/read-cvmfs.keytab"
chronic htgettoken -v -a vault.ligo.org -i igwn -r read-cvmfs-${USER} --scopes=read:/virgo --credkey=read-cvmfs-${USER}/robot/${USER}.ligo.caltech.edu --nooidc

# Rotate log files.
logrotate --state ~/.local/state/logrotate.status ~/.config/logrotate.conf
