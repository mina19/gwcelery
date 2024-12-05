.. highlight:: shell-session

Configuration
=============

Like any Celery application, GWCelery's configuration options are stored at run
time in a global configuration object, :obj:`gwcelery.app.conf`. There are
options for Celery itself such as options that affect the task and result
backends; these options are documented in the :ref:`celery:configuration`
section of the Celery manual.

The configuration object also holds all of the options that are specific to
GWCelery and affect the behavior of individual GWCelery tasks; examples include
the GraceDB service URLs, IGWN Alert groups, GCN hostnames, and frame file types and
channel names. For a list of all GWCelery-specific options, see the
API documentation for the :mod:`gwcelery.conf` module.

GWCelery provides five preset configurations, one for each GraceDB server
instance (production, deployment, testing, minikube or playground). The default
configuration preset is for the playground server,
``gracedb-playground.ligo.org``. The recommended way to select a different
preset is to set the :meth:`CELERY_CONFIG_MODULE
<celery.Celery.config_from_envvar>` environment variable before starting the
workers. For example, to configure GWCelery for production::

    $ export CELERY_CONFIG_MODULE=gwcelery.conf.production

Authentication
--------------

There are a few files that must be present in order to provide authentication
tokens for GraceDB and :doc:`IGWN Alert <igwn-alert:index>`.

.. rubric:: GraceDB

You must provide valid LSC DataGrid credentials in order for requests to the
GraceDB REST API to work. During development and testing, you can use your
personal credentials obtained from the `LSC DataGrid Client`_ by running
``ligo-proxy-init``. However, credentials obtained this way expire after a few
days or whenever your machine's temporary directory is wiped (e.g., at system
restart).

For production deployment, you should `obtain a robot certificate`_ and store
it in a location such as ``~/.globus/userkey.pem`` and
``~/.globus/usercert.pem``.

.. rubric:: IGWN Alert

You must provide a valid username and password for :doc:`IGWN Alert <igwn-alert:index>`. You can request an
account using the `SCiMMA Auth portal`_. To get started, see :doc:`IGWN Alert Userguide <igwn-alert:guide>`.
The IGWN Alert username and password should be stored in your `auth.toml`_ file.

.. rubric:: Kafka

You must provide a file named ``kafka_credential_map.json`` that maps
deployment specific usernames for Kafka credentials to the logical names given
in the configuration files. This file should be saved in the GWCelery XDG
config directory (``${HOME}/.config/gwcelery/`` by default on many Linux and
UNIX-like operating systems). An example file can be seen below::

    {
        "consumer": {
            "fermi": "user_one",
            "swift": "user_two"
        },
        "producer": {
            "gcn": "user_one",
            "scimma": "user_three"
        }
    }

Note that one user can be specified multiple times. ``hop auth`` must have
information about each user specified in this file. Every Kafka producer and
consumer configuration key must have an entry in this file.

.. rubric:: CVMFS read token

You must provide a valid credential for reading frames
from CVMFS for :mod:`detchar <gwcelery.tasks.detchar>` checks.
This is necessary if the data are no longer in the low-latency cache stored in ``/dev/shm``.
You can `obtain a robot certificate`_ in the form of a  `SciToken`_ keytab.
The keytab should be stored in the ``${HOME}`` directory, and named ``read-cvmfs.keytab``.

.. _`LSC DataGrid Client`: https://www.lsc-group.phys.uwm.edu/lscdatagrid/doc/installclient.html
.. _`obtain a robot certificate`: https://robots.ligo.org
.. _`SCiMMA Auth portal`: https://my.hop.scimma.org/
.. _`auth.toml`: https://hop-client.readthedocs.io/en/latest/user/auth.html#configuration
.. _`SciToken`: https://computing.docs.ligo.org/guide/auth/scitokens/

.. _redis-configuration:

Redis
-----

We recommend that you make the following settings in your Redis server
configuration file (which is located at :file:`/etc/redis.conf` on most
systems)::

    # Some GWCelery tasks transfer large payloads through Redis.
    # The default Redis client bandwidth limits are too small.
    client-output-buffer-limit normal 0 0 0
    client-output-buffer-limit slave 256mb 64mb 60
    client-output-buffer-limit pubsub 256mb 64mb 60

    # If worker nodes are only reachable on a specific network interface,
    # then make sure to bind any additional IP addresses here.
    bind 127.0.0.1 10.0.0.1  # replace 10.0.0.1 with address on cluster network

    # Disable RDB snapshots.
    save ""

    # Enable appendonly snapshots.
    appendonly yes

If you have to make any changes to your Redis configuration, be sure to restart
the Redis daemon.

Cron
----

For deployments of GWCelery at
`LIGO Data Grid computing sites <https://computing.docs.ligo.org/guide/computing-centres/ldg/>`_,
it is recommended that you configure :manpage:`cron <cron(8)>` to call the
script ``cron.sh`` once per hour by adding the following to your
:manpage:`crontab <crontab(1)>`::

    @hourly $HOME/cron.sh

This script automatically renews credentials and rotates log files.
