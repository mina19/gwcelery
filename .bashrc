# .bash_profile for deployment on LSC DataGrid clusters.
# Run at the start of an interactive shell (including a login shell).

# Source global definitions.
if [ -f /etc/bashrc ]; then
    . /etc/bashrc
fi

# Create log directories.
mkdir -p $HOME/.local/state/log

# Add user site directory to the PATH. On Linux, this is usuall ~/.local/bin.
export PATH="$(python3.11 -m site --user-base)/bin${PATH+:${PATH}}"

# Disable OpenMP, MKL, and OpenBLAS threading by default.
# In this environment, it will be enabled selectively by processes that use it.
export OMP_NUM_THREADS=1
export MKL_NUM_THREADS=1
export OPENBLAS_NUM_THREADS=1

# Use mpich for parameter estimation.
module load mpi/mpich-x86_64

# Unless the user has set `GSSAPIDelegateCredentials no` in their ~/.ssh/config
# file, their Globus certificate will be copied in when they log in, shadowing
# the robot certificate. Set these environment variables to override.
#
# Note: according to SSH_CONFIG(5), the default for `GSSAPIDelegateCredentials`
# is `no`. That's obviously incorrect!
export X509_USER_CERT="$HOME/.globus/usercert.pem"
export X509_USER_KEY="$HOME/.globus/userkey.pem"

# Configuration for GWCelery web applications.
export FLASK_RUN_PORT=5556
export FLASK_URL_PREFIX=/gwcelery
export FLOWER_PORT=5555
export FLOWER_URL_PREFIX=/flower

# GWCelery configuration-dependent instance variables.
case "${USER}" in
emfollow)
    export CELERY_CONFIG_MODULE="gwcelery.conf.production"
    ;;
emfollow-playground)
    export CELERY_CONFIG_MODULE="gwcelery.conf.playground"
    ;;
emfollow-test)
    export CELERY_CONFIG_MODULE="gwcelery.conf.test"
    ;;
emfollow-dev)
    export CELERY_CONFIG_MODULE="gwcelery.conf.dev"
    ;;
esac

# HTGETTOKENOPTS for passing through to condor
export HTGETTOKENOPTS="--vaultserver vault.ligo.org --issuer igwn --role read-cvmfs-${USER} --credkey read-cvmfs-${USER}/robot/${USER}.ligo.caltech.edu --nooidc"
