Operations
==========

Preliminary steps
-----------------

#. In order to perform some of the operations listed below, you need to be able to login to the *emfollow.ligo.caltech.edu* machine. To be granted permission, please send a mail to the low-latency chairs.
#. Some critical operations, like manually restarting GWCelery or removing HTCondor jobs, require you to login to that machine as the privileged user *emfollow*. These operations are reserved to experts. In case you need to be able to login as privileged user, please send a mail to the low-latency chairs.
#. To login to the *emfollow* machine, you need to provide a public SSH key by following `these instructions <https://ldg.ligo.org/ldg/manage_ssh/>`_.

Monitor the pipeline
--------------------

TODO

Monitor the HTCondor queue
--------------------------

You need to login to the *emfollow.ligo.caltech.edu* machine as unprivileged user:

``ssh albert.einstein@emfollow.ligo.caltech.edu``

and run the command:

``gwcelery condor q`` **TODO:** this command does not work if I login as unprivileged user

.. image:: _static/condor-queue.png
      :alt: Status of the HTCondor queue.

The 6th column of the ouput gives you the status of each job, which can have the following values:

#. R = running

#. H =  on hold

#. I = idle (waiting for a machine to execute on)

#. C = completed

#. X = removed

#. S = suspended (execution of a running job temporarily suspended on execute node)

#. < = transferring input (or queued to do so)

#.  > = transferring output (or queued to do so).

The last output column is the name of the worker, which needs to be reported to the experts in case of a job not in running state.
To better understand why a job is not in a running state, you can use the command:

``condor_q <job-id> -analyze``
 

Monitor the workflow execution
------------------------------

TODO

Diagnose issues
---------------

TODO


Restart the pipeline
--------------------

.. danger::
   Tasks to be executed only by an EXPERT or under her supervision.
   Note that the deployed version is from a specific commit/tag. DON'T perform git operations like git pull/git checkout that can change this.

GWCelery should be started/stopped using GitLab's continuous deployment, as explained in the :doc:`deployment` section. 

**TODO:** environments need to be fixed (https://git.ligo.org/emfollow/gwcelery/-/issues/610).

In case of problems with GitLab, the pipleine can be manually started/stopped in the following way:

#. login to the *emfollow* machine:

   ``ssh emfollow@emfollow.ligo.caltech.edu``

   (There is a confirmation page, respond *Yes*.)

   .. image:: _static/emfollow-login.png
      :alt: Confirmation page for the emfollow.ligo.caltech.edu machine.

#. remove the HTCondor jobs:

   ``gwcelery condor rm``

   Some jobs might refuse to be removed gracefully. Check this with:

   ``gwcelery condor q`` 

   If there are workers stuck with an *X* status. Remove them with:

   ``condor_rm -forcex <job-id>``

#. Resubmit the deployment that was running with:

   ``cd gwcelery``  **TODO:** is this needed?

   ``gwcelery condor submit`` 


