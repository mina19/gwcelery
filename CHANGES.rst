Changelog
=========

2.6.0 (unreleased)
------------------

-   Enabled SSM triggers and HasSSM source property

-   Rework determining the preferred external event using a keyfunc method
    similar to the superevent manager. This now prioritizes, in order, by
    whether there exists a previous event, whether the event is likely real,
    whether this event has passed publishing threshold, whether the search
    is prioritized, whether the space-time joint FAR is better, and whether
    the temporal joint FAR is better.

-   Check whether MOC to assign proper filename when downloading external
    sky maps.

-   Launch Fermi sky map requests only after ten minutes following the FINAL
    notice. This more closely matches the availability of these sky maps and
    prevents a significant number of failed calls, both retryable and those
    that will fail due to no sky map being available (e.g. after a FLIGHT notice
    with no FINAL notice) compared to the previous method.

-   When creating combined sky maps, set any negative values in the flattened
    external sky map to zero. This at least prevents erroneous results when
    we receive a sky map with these invalid values.

-   Roll back ligo-followup-advocate to 1.2.9 until SSM triggers are planned
    in production.

-   Direct GWSkyNet tasks to the ``openmp`` queue and retire the old ``skynet``
    queue. The dedicated ``gwskynet`` queue was necessary due to high memory
    usage that was fixed in GWSkyNet 2.5.1.

-   Reallocate the tasks for flattening and unflattening sky maps to a
    dedicated Celery queue for high memory usage tasks. This should prevent
    out-of-memory conditions that had resulted from these tasks being routed in
    round-robin fashion to workers in the high-concurrency general-purpose
    queue.

-   Fix coincidence search so SubGRB events can only be found in coincidence
    with CBC-like events (group is CBC or from CWB BBH search).

-   Don't download p-astro file for SSM alert canvas, since they are not required
    by policy.

-   Automatically rotate and compress log files to avoid running out of disk
    space. Logs are rotated and compressed once per week and deleted after one
    month.

-   Logs are now stored in the directory ``~/.local/state/log``, which is the
    per-user equivalent of ``/var/log`` (at least according to the conventions
    of `systemd <https://www.freedesktop.org/software/systemd/man/latest/systemd.unit.html#Specifiers>`_).

-   Rename the certificate renewal script from ``renew-cert.sh`` to ``cron.sh``
    to reflect the fact that it is now responsible for additional maintenance
    tasks including log rotation.

-   Fixed the broken link at the combined sky map png which was pointing to a
    wrong URL.

-   Plot external sky maps with contour lines and annotate with contained
    areas; remove central RA/dec position.

-   Drop support for Python 3.10, conforming to
    `SPEC 0 — Minimum Supported Dependencies <https://scientific-python.org/specs/spec-0000/>`_.

-   Reorganize RAVEN workflow to generalize IGWN alert sky map handler and
    kafka JSON to VOEvent converter to be used by multiple pipelines in the
    future.

2.5.2 "Cactus cat" (2024-11-18)
-------------------------------

-   Update ``bilby_pipe`` to v1.4.2, ``GWSkyNet`` to v2.5.1, and
    ``ligo.skymap`` to v2.1.2.

-   Roll back ``ligo-followup-advocate`` to 1.2.9 until SSM triggers are
    planned in production.

-   Change all external ingestion handlers and functions to concurrency of 1.

2.5.1 "Cactus cat" (2024-08-20)
-------------------------------

-   Update trials factor to account for MLy and SPIIR joining O4b.

-   Fix bug related to pesummary task failing due to recently changed
    naming convention of calibration filenames.

2.5.0 "Cactus cat" (2024-08-08)
-------------------------------

-   Update ``ligo-rrt-chat`` to v0.1.5, ``ligo-followup-advocate`` to v1.2.10, and
    ``ligo.skymap`` to v2.0.1.

-   Change functions calls to prepare for NumPy 2.0 release.

-   Drop support for Python 3.9 and switch from Python 3.9 to Python 3.11 for
    deployments for all tiers. This is to remain up to date with version support
    policies of Numpy, Astropy, and ligo.skymap, some of which have already
    dropped support for Python 3.9.

-   Add a GitLab issue template to include new external pipelines to RAVEN.

-   Switch to using ``igwn-alert`` client's listener, since most of functionality
    used in our implementation is now present in the upstream client.

-   Use integer times for creating caches for omegascans, since too much precision
    results in problems in loading in TimeSeries.

-   Fix bug where FloatingPointErrors in omegascans would not create the fig
    variable, which is later referenced.

-   Switch external event listeners to GCN Classic over Kafka.

-   Move CVMFS auth token retrieval from our crontab to renew-cert.sh to fix a race
    condition that caused authenticated GraceDB operations to fail in production.

-   Require specific format for all GraceDB event and superevent IDs in the
    dashboard.

2.4.4 "Orabou" (2024-06-14)
---------------------------

-   Fix bug in ``raven.py`` that prevented ``EM_COINC`` being applied for targeted
    subthreshold GRBs.

2.4.3 "Orabou" (2024-06-05)
---------------------------

-   Update to ``ligo-followup-advocate`` v1.2.9.

-   Revert to custom IGWN alert that was in use until long-term
    stability with the new client is demonstrated.

2.4.2 "Orabou" (2024-06-04)
---------------------------

-   Switch to using ``igwn-alert`` client's listener, since most of functionality
    used in our implementation is now present in the upstream client.

-   Bump bilby_pipe to 1.3.2 to fix calibration file lookups

-   Add search with subthreshold GRB events for CWB-BBH GW Superevents.

2.4.1 "Orabou" (2024-05-30)
---------------------------

-   Add GWSkyNet 2.4.1 to avoid installing tensorflow-cpu >= 2.16.1.
    Clairfy the version of annotated skymaps and quantities in the uploads.
    Add public tag to GWSkyNet's uploads.

-   Reduce Burst trials by one removing MLy from trials factor count.

-   Add ``DQR_REQUEST`` label with a countdown for significant early-warning
    events.

-   Increase tukey window roll off to 1s to reduce biases due to windowing in bilby.

-   Increase request memory for online PE with IMRPhenomXPHM waveform model.

-   Enable parameter estimation on burst-BBH triggers associated with
    low-significance CBC triggers.

-   Update ROTA responsibilities in documentation.

-   Remove AGILE from the RAVEN workflow since it has been decommissioned.

-   Retry email bootstep when IMAP connection is reset.

-   Add gracedb client side 409 error code to the list of retries.

-   Don't log omegascan FloatingPointErrors to Sentry since we know that they
    come from data which is only zeros (e.g., Virgo out of observing).

-   Filter Burst-BBH superevents from SNEWS coincidence searches.

-   Don't apply EM_COINC if external event is SubGRBTargeted and vetoed by
    NOT_GRB.

-   Require no spaces and specific version format for filenames when sending
    update notices

-   Serialize changing of preferred external event.

-   Update Nagios URL and screenshot in the documentation.

2.3.6 "Champ" (2024-04-30)
--------------------------

-   Update trials factor to account for MLy joining O4b.

-   Update ligo-skymap to 2.0.0.

2.3.5 "Champ" (2024-04-11)
--------------------------

-   Remove oLIB from trials factor for beginning of O4b.

2.3.4 "Champ" (2024-04-04)
--------------------------

-   Fix bug that sent cWB-BBH notices as malformed Burst notices.

-   Change Bilby accounting group user back to to soichiro.morisaki.

2.3.3 "Champ" (2024-04-01)
--------------------------

-   Change Bilby accounting group user to cody.messick as temporary workaround
    while KAGRA accounts are in flux.

2.3.2 "Champ" (2024-03-22)
--------------------------

-   Enable public alerts on production.

2.3.1 "Champ" (2024-03-22)
--------------------------

-   Moved ``submit_rapidpe`` to ``inference.py`` and refactored to delete
    ``base_submit``.

-   Add rates of external triggers as configuration variables. This allows us
    to make adjustments without a ligo-raven release.

-   Increase the number of parallel bilby jobs to increase the number of
    posterior samples.

-   Add search field to alert threshold and trials factor configuration
    variables.

-   Fix bug when creating combined sky maps in the targeted search, being
    loaded in the ring ordering rather than nested.

-   Update trials factor for O4b.

-   Pin bilby_pipe to bilby_pipe==1.3.1.

2.2.1 "Sheepsquatch" (2024-03-08)
---------------------------------

-   Include p_astro and em_bright in alert payload for burst-cwb-bbh trigger.

-   Produce em_bright.json for burst-cwb-bbh triggers.

-   Add acceptance check to if dependencies have been approved by SCCB and if
    Bilby PE summary is available.

-   Do not add events involving KAGRA to superevents.

-   Require pesummary>=1.0.2 to fix bug in postprocessing of bilby results.

-   Update gwpy>=3.0.8 to fix issue with discontiguous lal caches.

-   Change the worker concurrency to 32 and set ``max-memory-per-child`` to
    2GB.

-   Document librarian ROTA duties.

-   Make the ``dev`` configuration the default for testing.

-   Allow SSM events to be added to/create superevents. Disable em-bright
    on SSM events until implementation exists.

-   If an official Fermi sky map is already uploaded to external event, block
    future uploads.

-   Ignore events from VTInjection search with regards to annotations and superevent
    creation.

-   Disable sending alerts on production, leave them on for other instances.

-   Add a form to the Flask console to create BAYESTAR sky maps with selected
    detectors disabled.

-   Upgrade RapidPE/RIFT to v0.6.7, RapidPE to v0.1.1.  Don't set ``getenv =
    True`` in HTCondor, instead set ``getenv`` and ``environment`` options
    explicitly.  Modify ``condor._submit``, ``condor.base_submit``, and
    ``condor.submit_rapidpe`` to accept an optional list for ``getenv``, to
    pass to the ``condor_submit`` executable.  Include pastro from RapidPE/RIFT
    in the second preliminary notice and initial notice+circular.

-   Add instruments and objid fields to the header of sky map files produced
    from posterior samples.

-   Ignore EarlyWarning triggers when launching RapidPE/RIFT.

-   Upgrade RapidPE/RIFT to v0.6.8.

2.1.10 "Bunyip" (12-05-2023)
----------------------------

-   Re-apply all v2.1.8 "Ahool" changes after emergency release of v2.1.9
    "Ghost Deer".

-   Prevent uploads of INTEGRAL Test events to the production server to avoid
    unnecessary polls of old GW data and use of resources.

-   Increase coverage for RAVEN test to nearly 100%; add notice type in
    gracedb log for external sky maps regarding their source.

-   Update documentation for RAVEN; add missing documentation for various
    other tasks.

-   Set a 600 second hard time limit for ``superevents.process`` tasks.

-   Set pipeline preferred events for low significance alerts. Previously,
    pipeline preferred events were only set for high significance alerts.

-   Check for stale caches by checking both the start-time and end-time
    of the segment. Previously only the start-time would be checked. Add
    Nagios check for existence of llhoft data.

-   Disable parameter estimation for offline G-events.

-   Add ``condor.submit_rapidpe``, a submit task dedicated for RapidPE with
    lower ``retry_backoff_max`` than other PE pipelines.

-   Get the ``getenv`` value from the ``gwcelery.tasks.inference`` module
    variable instead of hardcoding in condor sub file.

-   Modify rapidpe config template to use specific options for high snr
    triggers. Turn-off cProfile for production and playground.

-   Update bilby to 2.2.2 and bilby_pipe to 1.3.0; ensure that environment
    variables are passed correctly when using ``HTCondor>=10.7``.

-   Upgrade RapidPE/RIFT to v0.6.4, RapidPE to v0.1.1.

-   Update deployment to install ``gwcelery`` using ``pip`` instead of
    ``poetry`` because the latter installs the project in editable mode.

-   Update ligo-followup-advocate to 1.2.8.

2.1.9 "Ghost Deer" (11-27-2023)
-------------------------------

-   Roll back all v2.1.8 "Ahool" changes for an emergency release of v2.1.7
    "Spring-heeled Jack" with a bug fix for Out-of-Memory issues.

-   Require IGWN-Alert >= 0.3.0.

-   Add a separate ``em-bright`` worker for re-routing em-bright tasks.

2.1.8 "Ahool" (09-01-2023)
--------------------------

-   Sort Python packages on Flask webapp by name for readability.

-   Update Bilby to 2.1.2; fixes a post-processing bug that can impact
    distance posteriors for low-SNR events.

-   Update bilby_pipe to 1.2.0; address the same bug as for Bilby and fix a
    typo in frame paths on playground.

-   Update release template to refer and point to test instance rather than
    playground.

-   Modify project ``.bashrc`` to avoid sourcing a conda environment. Update
    deploy-common stage to install ``gwcelery`` using ``poetry``.

-   Use the central time for querying events in the targeted subthreshold
    search. This is an improvement since the starting time of low SNR GRB
    candidates tends to be unreliable.

-   Use ``ligo-skymap`` API to create bayestar coherence plots.

2.1.7 "Spring-heeled Jack" (08-21-2023)
---------------------------------------

-   Specify GW group when triggering RAVEN pipeline if not given. This fixes a
    bug introduced when allowing the SubGRBTargeted search to find both CBC and
    Burst events.

-   Add condition that prevents a RAVEN alert if the FAR is negative. This is
    meant to add clarification that a SubGRBTargeted GRB with a FAR higher
    than the given threshold (which leads to a negative joint FAR), or any
    other situation that leads to a negative FAR does not pass publishing
    conditions.

-   Update ligo-followup-advocate to 1.2.6; enable option to remove line
    wrapping in order to conform to new GCN circular recommendations.

-   Update rapidpe-rift-pipe to 0.6.3. Rapidpe will run on triggers from all
    pipelines. In this version ``pTerrestrial`` is taken from the preferred
    event instead of calculating it from rankingstatpdf. Removed  config
    options ``far_threshold`` and ``rankstat_pdf_file`` that were used for
    ``pTerrestrial`` calculation. Using config option ``total-points`` instead
    of ``points-per-side`` to set the required number of grid points in the
    initial intrinsic grid. ``total-points`` will make sure that we get close
    to the required number of grid points. Corrected the time-offset of MDC to
    match the current MDC. Setting the minimum frequency ``fmiin-template``
    to 20Hz to match the minimum frequency used by bilby.

-   Fix typo to block coherence plots for combined sky maps, since these are
    redundant to their GW counterparts.

-   Add subthreshold targeted events from Fermi and Swift, as well as
    subthreshold untargeted events from Fermi to the O3 MDC replay.

2.1.6 "Champ" (08-10-2023)
--------------------------

-   Prevent Sentry from triggering on email notice validator since an
    error message is already logged to gracedb in case of failure.

-   Lower omegascan minimum frequency to 10 Hz from 20 Hz to better
    frequencies used by searches.

-   Change the superevent nagios check behavior to check against created
    time instead of ``t_0``.

-   Install bleeding edge version of flower monitor until next point
    release fixing ``url-prefix`` option. For reference, see
    https://github.com/mher/flower/issues/1316

-   Don't attempt to download ``em_bright`` and ``p_astro`` files from events
    that don't provide them. This fixes the case where burst only alerts
    contained garbled information in the classification and properties section.

-   Expose PE skymap images, HTML, and gzip FITS when update notice is sent.

-   Prevent public tag being applied automatically to ``RapidPE_RIFT.p_astro.json``.
    Therefore this is not included automatically at the time of sending initial alert.

2.1.5 "Ningen" (08-01-2023)
---------------------------

-   Add "online" label to PESummary result page from online Bilby.

-   Increase tolerance duration for superevent existence nagios check in
    the last hour from 1 min to 10 mins.

-   Add error handling to hop-client consumers, and restart the consumer
    on non-fatal errors.

-   Ensure that the content of ``coincidence_far.json`` is in JSON format
    in ``views.py``.

2.1.4 "Mokele-Mbembe" (07-26-2023)
----------------------------------

-   Use upload instead of create_log and update t_0 in update_if_dqok.

-   Add nagios check for presence of recent MDC superevents.

-   Add new celery worker for Kafka consumer.

-   For the targeted search, add configuration variables for FAR thresholds and
    additional trials factors. Prevent GRB candidates that fall outside of FAR
    thresholds from triggering an alert.

-   Update bilby_pipe to 1.1.2.

-   Updated RapidPE/RIFT Pipeline to 0.5.3, replacing errors from absent GstLAL
    triggers with a graceful failure.  Updated RIFT to 0.0.15.9 to fix
    compatibility with newer NumPy versions.  Updated LALSuite to 7.16 to fix
    bug in `XLALSimInspiralTDModesFromPolarizations` causing RIFT to fail.
    Setting appropriate prod/dev accounting group depending on GraceDB URL.

-   Disable online PE for MDC events from MBTA until multiple data sources can
    be handled.

-   Ensure consistent clim between omegascans and annotate plots with Q used.
    Add 2 seconds to start/end of data query to avoid whitening problems.

-   Change Kafka consumer configuration to listen to production Swift topic on
    all deployments and to listen to Fermi production topic on production
    deployment.

-   Disable automatic playground deployment; enable automatic test deployment.

-   Launch subthreshold targeted search for Burst events as well, using the same
    search windows.

-   Update ligo-followup-advocate to 1.2.5.

2.1.3 "Thunderbird" (2023-06-24)
--------------------------------

-   Update to celery version 5.3.

-   Update ligo.em-bright to version 1.1.4.post2, ligo-followup-advocate
    to version 1.2.4.

-   Apply FROZEN_LABEL inside the handle_superevent as a part of the alert
    canvas so that they are retries for RetryableHTTPErrors.

-   Add ingestion of Fermi and Swift subthreshold targeted events. Generalize
    shared creation and replacement workflow used by various external trigger
    listeners into a single function.

-   Handle HTTP Error raised while grabbing Fermi skymaps.

-   Add handling for ValueErrors in detchar.check_vector to allow for frames
    without the requested channel.

-   Log failed omegascan exceptions to Sentry.

-   Change logging level of StateVector reading errors in detchar.check_vector
    back to log.exception now that https://github.com/gwpy/gwpy/issues/1211
    is fixed.

2.1.2 "Ogopogo" (2023-06-09)
----------------------------

-   Update the low-significance threshold to be 2/day pre-trials factor. Update
    low-siginificance trials factors to 7.

-   Do not apply HIGH_PROFILE label to less-significant alerts.

-   Do not make pe skymap public automatically before approved.

-   Make image created when omegascan fails smaller.

-   Remove "overall state of detectors" line from detchar vector table,
    since without Virgo being online it will always be "bad".

-   Check the iDQ OK vector before checking the value of iDQ timeseries.

-   Upgrade bilby_pipe to v1.1.1.

-   Enable SCiMMA kafka on minikube.

-   Use real KafkaError methods in delivery callback.

-   Use production accounting tag for bilby running on real events.

-   Disable querying data before starting PE as bilby now uses kafka data
    stream and PE timeout is long enough for low-latency frame data to be
    produced.

-   Move RAVEN tasks to main worker and make GCN ingestion sequential.

-   Update low-significance FAR threshold and trials factors to ensure these
    will go out at a rate of 2/day pre-trials factors.

-   Limit the number of MDC BBH bilby jobs to less than 5 to save disk usage.

-   Upgrade lalsuite to v7.15.

-   Updated rapidpe-rift-pipe version to 0.5.1. rapidpe will be
    submitted on superevents with gstlal/non-gstlal preferred event.
    Launching rapidpe 30s after merger. `query_shm=True` will run on lower
    latency data on `/dev/shm/kafka/` if it is available, if not it will
    default to `/ifocache/llcache/kafka/`.

-   When updating the preferred event, also update t_0.

2.1.1 "Lone Island Mountain Devil" (2023-05-29)
-----------------------------------------------

-   Upgrade ligo-followup-advocate to v1.2.3.

-   Bump ligo.em-bright to v1.1.3 to be compatible with schema of online PE
    files with spinning waveforms.

-   Pin astropy to "!=5.3" until a bug plotting omega scans with gwpy is
    resolved.

2.1.0 "Jersey Devil" (2023-05-19)
---------------------------------

-   Add GWSkyNet to annotate CBC all-sky search superevents. Only the
    superevents that meet the following conditions will be annotated by
    GWSkyNet: 1. A network of signal-to-noise ratio of greater
    than or equal to 7.0; 2. The SNRs in at least two detectors are no less
    than 4.5; 3. The FAR is below 1 / 21600Hz. In addition, superevents
    that meet the above conditions will also be annotated if there is a change
    of preferred event.

-   Only check iDQ for detectors that are on.

-   Update IGWN alert authentication documentation.

-   Clean up large bilby data files after the run for MDC injections.

-   Add pe tag to pesummary uploads.

-   Add new emfollow-dev deployment.

-   Ignore SSM search for superevents until decision about dedicated notice type
    and other details from OpsDiv are decided.

-   Update rapidpe-rift-pipe to v0.3.0. Adding config options to calculate
    pastro. Uploading rapidpe pastro to gracedb. Adding a link to
    rapidpe summary page in logs. Moving the config option to cprofile the
    jobs to the General section of config. Increasing the number of grid points
    per intrinsic coordinate to 10.  Adding a config option to specify
    the type of run (`online` vs `o3replay`) when choosing channel names, the
    same as `bilby_pipe` does.  Modified `inference.py` to set this config
    option based on the GraceDB URL.

-   Increase request memory of bilby runs for BBH.

-   Update bilby filenames in acceptance test.

-   Reduce GW FAR requirement for SOG_READY to 1/10 years.

-   Handle ValueErrors from checking iDQ channels, such as if the configured
    iDQ channel name is not the same as that in the frames. This happened for
    S230518h during ER15 and broke the alert pipeline.

-   Update the Kafka topic that we consume Fermi alerts from.

-   Fix the triple circular uploads by rearranging the alerts canvas and
    moving tasks into the second group.

-   Add ability to create circular for medium-latency GRB followup by PyGRB
    and X-pipeline from the dashboard.

-   Add pe tag to EM-bright probabilities calculated from bilby samples.

-   Add an automatic retry for the check_vectors task if requested data is not
    yet available in the caches due to data transfer latency. This will retry
    every five seconds up to four times, for a maximum total of 20 seconds.

-   Rerun the RAVEN sky map comparison whenever the GW preferred event changes.

-   Upgrade bilby to v2.1.1

-   Update ligo-raven to v3.2 and ligo-followup-advocate to v1.2.2.

2.0.6 "Spaghetti Tree" (2023-05-10)
-----------------------------------

-   Add `HIGH_PROFILE` label for rapid response team.

-   Ensure that external events are not already associated with a superevent
    when triggering Raven off of superevents.

-   Add minimum resolution for external sky maps.

-   Remove clutter from omegascan pngs and lower dpi to 150 to reduce
    file size.

-   Retry GCN circular creation upon 408 or 409 errors, likely due to AWS
    issues for GraceDB.

-   Add Kafka consumer bootstep.

-   Skip plotting sky map overlap integral if coinc_far_dict is empty, which
    occurs for SNEWS coincidences.

-   Update iDQ channel configuration for O4 iDQ channels. The playground
    config will continue to use the O3 channels to support the O3 MDC replay.

-   Update bilby settings. Set the chirp mass boundary between BNS and
    potential NSBH to 1.465Msun, computed from component masses of 3Msun and
    1Msun, and use low-spin IMRPhenomD below and high-spin IMRPhenomPv2 above.
    For high mass triggers, use IMRPhenomXPHM. Use these "production" settings
    by default for MDC and production analyses, and we no longer have multiple
    bilby runs on a single event. Increase the time limit of condor job to 12
    hours.

-   Bump pesummary version to 1.0.0. Disable ligo skymap and use 6 cpus and
    online-PE dedicated nodes to speed up the pesummary postprocess.

-   Enable public alerts in production.

-   Disable Kafka consumer bootstep in the production deployment.

-   Fix bug where Kafka alerts were using combined sky map from the external
    event rather than the superevent.

-   Fix bug where the external sky map was created with out-of-date external
    GCN information.

-   Require em-bright >= 1.1.2.


2.0.5 "Mothman" (2023-04-20)
----------------------------

-   Add Mattermost channel creation for the RRT to discuss a superevent
    candidate.

-   Use Online_PE_MDC nodes for bilby jobs on O3-Replay injections.

-   Do not remove PE run directory after the run completes or fails.

-   Filter external events based on ``External`` group in ``superevents.py``,
    instead of logic based on substring matching.

-   Increase test coverage for views.py and fix manual mock coincidence
    injections.

-   Bump ligo.em-bright version to v1.1.1.

-   Bump ligo-followup-advocate version to v1.2.1.

-   Also update the preferred external event when issuing a manual RAVEN alert.

-   Skip less significant alert workflow in case of both significant and EW
    triggers. Previous workflow only accounted for blocking in presence of
    significant events.

-   Update rapidpe-rift-pipe version to 0.0.12. Use mchirp and q as the
    coordinates for the rectilinear intrinsic grid. Update accounting_group to
    ligo.dev.o4.cbc.pe.lalinferencerapid and add accouting_group_user. Add
    an option to map the events to an injection. Upload only a select set
    of log files to gracedb.

-   Prevent alternative collating method for O3 replay and MDC events with INTEGRAL.

-   Pick RAVEN_ALERT preferred external event over one that doesn't pass the joint
    alert publishing criteria.

-   Avoid updating values in the combined sky map if missing in the GW sky map.

-   Catch bug if instrument not in external sky map header. This could occur
    if grabbing a sky map from an external URL such as from Fermi-GBM that
    lacks this field.

-   Prevent external event from switching superevents.

-   Only plot sky map overlap with prefered external event when RAVEN_ALERT
    label is applied.

-   Populate the duration and central_frequency fields in Kafka notices for
    burst events.

-   Add periodic SNEWS MDC events to test the corresponding IGWN alert
    listener.

-   Use sky map from preferred event rather than superevent, triggering off
    EM_READY label instead of SKYMAP_READY. If SKYMAP_READY is applied or if a
    sky map file is added to the superevent, we will once again try to get the
    GW sky map from the superevent.

-   Update bilby and bilby_pipe to 2.1.0 and 1.0.10 respectively. The number of
    spline nodes for calibration errors is increased to 10 thanks to the bilby
    optimizations. Change sampler settings into naccept=60, nlive=500.

-   Add automatic PESummary task to postprocess bilby parameter estimation
    results.

-   Indicate dispatch of first less-significant alert with
    ``LOW_SIGNIF_PRELIM_SENT``. Launch timer for revision and send second less
    significant alert with the same label conditioned on blocking labels
    indicating significant alert or advocate action.

-   Replace ``EM_Selected`` and ``EM_SelectedConfident`` labels with
    ``LOW_SIGNIF_LOCKED`` and ``SIGNIF_LOCKED``.

-   Fix repeating of raven alert publishing criteria met log message.

-   Don't compute p-astro for spiir/AllSky because it now computes and uploads
    its own.

-   Prevent repeating of sky map comparison pipeline with the secondary
    flattened sky maps.

2.0.4 "Skunk Ape" (2023-03-28)
------------------------------

-   Add dashboard button to download sky map from a URL and upload to an
    external event.

-   Set pipeline preferred events after revising preferred event. Provide dashboard to set them.

-   Remove the ``prelimimary_alert_timeout`` variable and workflow, since the
    value is set to zero, and the pathway is no longer used.

-   Fix bugs in online bilby PE.

-   Use more production-like settings in online bilby PE for MDC triggers.

-   Clean up the superevent before sending significant alert.

-   Explicitly rank publishability of significant events higher than less-significant
    events ones in ``superevents.keyfunc`` since these are two distinct categories
    of alerts.

-   Remove infinite far threshold for earlywarning alerts for playground configuration.
    Set far threshold of 1/30 days for early warning triggers in conf/__init__.py.

-   Update the significant far threshold from 1/60 days to 1/30 days in conf/__init__.py.

-   Enable less-significant public alerts. Change the behavior for the ``EM_Selected``
    label to lauch less-significant alert pipeline.
    Use the ``EM_SelectedConfident`` label to indicate completeness and passing significant
    full-bandwidth event; this starts the "significant" alert pipeline. Less-significant
    alert pipeline is blocked by the presence of ``EM_SelectedConfident``.

-   Enable public early warning alerts. The superevent manager now applies the
    ``EARLY_WARNING`` label to the superevent when a significant EW event is added
    to the superevent. The automated pipeline is launched and is blocked before sending
    if ``EM_SelectedConfident`` is found to be applied.

-   Add O3 replay MDC testing with RAVEN pipeline. This will run on the
    emfollow-playground server, creating mock coincidences with a frequency
    given by the ``joint_O3_replay_freq`` variable.

-   Require sky map information to publish coincidence with a GRB candidate.

-   Fix collation of INTEGRAL notices by getting the GCN ID from the IVORN
    field.

-   Add criteria when choosing new preferred external event that real events
    are preferred over those likely not to be real, and SNEWS events won't be
    overwritten by GRBs.

-   Update external coincidence to update circulars.

-   Relaunch RAVEN sky map comparison whenever a new GW or external sky map is
    available. Also copy over to the superevent if an alert has already been
    sent out.

-   Integrate MLy into superevent manager.

-   Update bilby to 2.0.0 and bilby_pipe to 1.0.8. Use ROQ bases dedicated for
    BNS mass region for low-mass signal. Switch to acceptance-walk method for
    dynesty sampling, and use naccept=10, nlive=500, and nparallel=2 for
    quick-BNS run, and naccept=20, nlive=1000, and nparallel=2 for the other
    runs.

-   Adjust arguments for ``ligo-skymap-from-samples``: set the random seed to
    make the results reproducible and set the maximum number of points to limit
    run time.

-   Add condor accounting user tag to PE jobs.

-   Update rapidpe-rift-pipe to 0.0.8. This finds the initial grid region based
    on gstlal search biases. Fixes spin components to gstlal trigger spin. Offsets
    likelihood by 0.5*snr**2 to avoid numpy overflow. Reduced number of extrinsic
    samples and the number of extrinsic samples that get saved.

-   Pin redis<4.5.2 due to a potential bug discovered in redis 4.5.2.

-   Force omegascan plots to all use the same color scheme.

-   Plot longer timespan before event and shorter timespan after event in
    omegascans.

-   Take the minimum false alarm probability instead of the maximum when
    checking if iDQ false alarm probabilities pass the iDQ
    false-alarm-probability threshold.

-   Do not change current directory when setting up rapidpe dag.

-   Update condor memory request and accounting tag for sky map jobs.

-   Add a dedicated Celery worker for tasks that use Python multiprocessing for
    parallelism, separate from the queue for OpenMP parallelism. This should
    work around an issue where ``ligo-skymap-from-samples`` was using _both_
    OpenMP and Python multiprocessing, running slowly and causing Condor job
    evictions due to excessive memory usage.

-   Add duration and central_frequency fields to Kafka notices. The fields are
    set to None for now.

-   Set the ``Significant`` field in VOEvent XML packets and Kafka notices.

-   Import Kafka notice schema from the new igwn-gwalert-schema library.

-   Drop support for Python 3.8.

2.0.3 "Ugly Merman" (2023-02-16)
--------------------------------

-   Require bilby_pipe>=1.0.7.

-   Require matplotlib<3.7 to fix bleeding edge dependencies tests.

-   Use multi-order GW sky maps to produce combined sky maps and add combined
    sky maps to alerts alerts. The presence of the `COMBINEDSKYMAP_READY` label
    indicates the combined sky map is now available in that external event or
    superevent. Only copy a combined sky map to the superevent when sending an
    alert if the preferred external event has one available.

-   Resurrect unit test `test_handle_superevent`.

-   Make igwn alert listener retry all non-fatal errors. This
    should be revisited once https://github.com/astronomy-commons/adc-streaming/issues/54
    is resolved.

-   Add `EARLY_WARNING_LABEL` symbol to superevents.py.

-   Remove subthreshold annotations from orchestrator canvases.

-   Fix race condition of multiple instances of the RAVEN pipeline by
    polling the superevent state before updating joint FAR.

-   Adjust PE event directory permissions.

-   Test SoG pipeline with MDC events.

-   Update ligo.em-bright to version 1.1.0.

-   Update BAYESTAR task to use PSD in ``coinc.xml`` file for all pipelines.

-   Convert cWB FITS files to multiorder format if not already in the
    multiorder format.

-   Don't compute p-astro for PyCBC/AllSky because it now computes
    and uploads its own.

-   Don't try to download psd.xml.gz in online PE, as it is no longer uploaded
    by any search pipelines.

-   Add functionality in superevent manager to decide confident vs. less significant
    alert criteria.

-   Re-purpose FROZEN_LABEL to mean subthreshold alert criteria is met. Add new
    SIGNIFICANT_LABEL to indicate presence of significant full-bandwidth trigger.
    Apply EARLY_WARNING_LABEL to superevent when there is a significant EW event.

2.0.2 "Flying Icarus" (2022-12-23)
----------------------------------

-   Change web directory for PE outputs. Use Online PE nodes for bilby. Do not upload lalinference ini files.

-   Add RIFT analysis only for gstlal triggers.

-   Use the exttring worker for RAVEN tasks.

-   Bump ligo.em-bright to 1.1.0.dev1 to add HasMassGap.
    Review page: https://git.ligo.org/emfollow/em-properties/em-bright/-/wikis/Mass-gap-review

-   Compute source properties uniformly across all CBC pipelines.

-   Don't compute p-astro for MBTA because they now compute and upload their
    own.

-   Change expected p-astro filename from ``p_astro.json`` to
    ``pipeline.p_astro.json``, where pipeline is the name of the pipeline that
    uploaded the event.

-   Indicate a joint CBC-GRB event should be used for a measurement of the
    speed of gravity by applying the SOG_READY label to the superevent. This pipeline is launched is the right conditions are met after ADVOK label.

-   Add button to apply RAVEN alert labels to flask app. This will manually
    trigger a RAVEN alert.

-   Update messages from RAVEN alert pipeline to be more informative.

-   Fix bug where updated GRB events couldn't create sky maps.

-   Filter out test GCNs from updating external events.

-   Update SNEWS ingestion to use canvas structure. This also fixes a bug
    where SNEWS tests events were not being ingested properly.

-   Fix datetime format in Avro and JSON notices so that they conform to
    ISO 8601. They were missing the ``T`` separating the date from the time
    and the final ``Z`` denoting the UTC time zone.

-   Set pipeline preferred events during superevent clean up.

2.0.1 "Alien Big Cat" (2022-10-26)
----------------------------------

-   Restrict ligo.em_bright to >=1.0.4 to accept new posterior samples format

-   Fix regression in superevent clean-up

-   Fix regression where subthreshold annotations could use stale data

-   Silence Sentry for adc-streaming log messages

2.0.0 "Akkorokamui" (2022-10-14)
--------------------------------

-   Introduce public alerts over Kafka via SCiMMA and the new GCN.

-   This release establishes the practice of naming GWCelery releases after
    `cryptids`__.

    __ https://en.wikipedia.org/wiki/List_of_cryptids

-   Send SIGKILL to vacate condor jobs that do not die promptly when condor_rm'ed.

-   Preferentially pick coincident SNEWS events over coincident GRB events.

-   Add HTTP 408 Request Timeout errors to list of errors that trigger a retry.

-   Ignore BBH/IMBH specific searches for GW-GRB burst searches.

-   Perform first2years MDC uploads asynchronously.

-   Adjust ``request_memory`` specification for condor submission of OpenMP workers.

-   Ignore noisy adc-streaming log messages due to frequent but harmless
    errors in the IGWN Alert listener.

-   Replace instances of "MBTAOnline" with "MBTA".

-   Remove gstlal from list of pipelines that upload PSDs in separate
    ``psd.xml.gz`` file. Update mock event uploads to include PSD in
    ``coinc.xml`` upload.

-   Disable LALInference parameter estimation and switch to Bilby as the main
    parameter estimation software.

-   Add bilby-pipe>=1.0.6 and gwdatafind>=1.1.1 dependency, and unpin pesummary
    to fix bilby workflow for O3-replay.

-   Disable parameter estimation for MDC events as it is currently broken for
    them.

-   Enable ingestion and processing of test SNEWS external events.

-   Run unit tests under Python 3.10.

-   Add ``DQR_REQUEST`` label to superevent after sending first preliminary
    alert.

-   Embed PSDs in first2years event uploads, matching the O4 configuration of most CBC pipelines.

-   Create external MDC events to test the RAVEN alert system. Test alerts
    include Fermi, Swift, INTEGRAL, and AGILE-MCAL. Add acceptance tests for
    the RAVEN alert system. Add options to use the superevent search field in
    coincident searches. Update the preferred external event based on the best
    available.

-   Report the URL of multi-resolution FITS files in GCN Notices and create
    flat-resolution files asynchronously.

-   Move functions in handle_grb_gcn to an asynchronous group to prevent
    detchar errors from interupting sky map generation.

-   Prevent sub-threhsold GRBs from overwriting high-threshold GRBs.

-   Listen to initial GBM alerts for earlier warning. Prevent these events
    from triggering alerts unless later updated.

-   Adjust Celery working concurrency settings: turn off autoscaling, turn
    on prefetching. This is seen to decrease the latency of most tasks.
    For example, the latency of ``gwcelery.tasks.gracedb.upload`` tasks is
    decreased by a median of about 0.5 s.

-   Set the preliminary alert timeout to 0 s.

-   Use multi-resolution GW sky maps when calculating the joint false alarm
    rate. Use single pixel RA/dec when evaluating for Swift coincidences.

-   Filter BBH/IMBH events from burst-GRB searches.

1.0.1 (2022-05-09)
------------------

-   Added ``request_disk`` specification for gwcelery condor submission

-   Bump ``p-astro`` to pre-release ``v1.0.0dev1``. This version is a stop gap to
    bring back the reference O3 hard-cut implementation of p-astro, and
    resolve the dependency issues. Will need a full release later.

-   Add ``ligo.em_bright>=0.1.5`` dependency.

-   Update MPICH module in deployment environment for RL8

-   Configure the playground environment to read O3ReplayMDC frames.

-   In O4, online CBC pipelines shall include the PSDs in the initial
    ``coinc.xml`` upload and shall not upload a separate ``psd.xml.gz`` file.
    Update the list of pipelines that have not yet made the transition and
    still require the old behavior (gstlal, spiir).

-   Set default RAVEN search to empty list to fix argument error.

-   Drop python3.7 support, add python3.9 testing.

-   Update to em-bright >=1.0 that implements EoS maginalization for HasNS
    and HasRemnant. An important difference compared to previous versions
    is that the trained classifiers are no longer stored as package data,
    but downloaded and cached using ``astropy.utils.data.download_file``.
    These are also loaded in module scope in em_bright, and therefore
    we are no longer required to pass them explicitly.

-   Update conda environment in bashrc to igwn-py39-20220317.

-   Fix threshold to correct scale in order to consider a Fermi GRB real.

-   Prevent external GCN notices with no sky map information (all zeros) from
    creating sky maps.

-   Pass --no-deps to pip during deployment because of a bug in pip and because
    the poetry lock file already contains all of the dependencies

-   Drop --use-feature=in-tree-build from pip call in deployment, as pip now
    does this by default and has deprecated this option

-   Switch to using IGWN alerts instead of LVAlert. Add ``igwn-alert``
    as a dependency.

1.0.0 (2022-01-21)
------------------

-   Update to Celery 5.

-   Rename branch ``master`` to ``main``.

-   Switch build, packaging, and deployment from setuptools+pipenv to poetry.

-   Use a date-tagged IGWN Conda environment to prevent unversioned changes
    to dependencies.

-   Some unit tests now use a live worker instead of "eager" mode. As a result,
    Celery's behavior in those unit tests is more similar to production, and
    therefore more likely to catch any concurrency bugs, race conditions, or
    deadlocks.

-   Rewrite GitLab CI pipeline to use the IGWN computing group's Python job
    templates.

-   Use ssh+kerberos instead of the now-defunct gsissh for unattended login to
    LDG hosts for deployment jobs in the GitLab CI pipeline.

-   Remove mock module imports from Sphinx configuration, for simpler and more
    robust documentation builds.

-   Increase the value of the Celery ``worker_proc_alive_timeout`` to 8 seconds
    in order to avoid unnecessarily killing workers that are slow to start up.

-   Remove workarounds that were in place to preserve order of results from
    groups of tasks, because Celery 5 now preserves result order automatically.

-   Require astropy >= 4.3.1 due to an upstream bug
    (https://github.com/astropy/astropy/issues/11879).

-   Fix a bug in configuration of the Jinja template directory.

-   Reduce queries to gracedb by RAVEN by passing event dictionaries directly.

-   Switch to use gracedb-sdk for RAVEN.

0.13.1 (2021-03-01)
-------------------

This release primarily updates versions of dependencies.

-   Pin celery to version 4.4.2 because version 4.4.4 breaks the GWCelery unit
    tests. (See https://git.ligo.org/emfollow/gwcelery/-/issues/348)

-   Require ligo-gracedb >= 2.7.5 to take advantage of connection pooling and
    pick up several bug fixes and regression fixes:

    -   https://git.ligo.org/lscsoft/gracedb-client/-/issues/24
    -   https://git.ligo.org/lscsoft/gracedb-client/-/issues/25
    -   https://git.ligo.org/lscsoft/gracedb-client/-/issues/28

-   Require gwpy >= 2.0.2 to work around a Matplotlib compatibility bug that
    was fixed in that version (see https://github.com/gwpy/gwpy/issues/1277).

-   Require LALSuite >= 6.82 to work around a segmentation fault that occurred
    with earlier versions of LALSuite and with versions of Numpy >= 1.20
    (see https://git.ligo.org/lscsoft/lalsuite/-/issues/414).

-   Update p_astro to version 0.8.2 and ligo.skymap to version 0.5.1.

-   Improve the robustness of detecting whether modules are being imported by
    Sphinx in order to work around some minor changes in the Readthedocs build
    process (see https://github.com/readthedocs/readthedocs.org/pull/7846).

-   Close Matplotlib figures that are created during tasks to avoid leaking
    references and memory.

-   Adapt to a change in the GraceDB server's API response for a request to
    create a label that already exists.

-   Set the matplotlib backend to ``agg`` in order to fix plot layout glitches
    that started with matplotlib 3.3.0 when ``plt.tight_layout`` became backend
    dependent (see https://github.com/matplotlib/matplotlib/pull/15221).

0.13.0 (2020-06-03)
-------------------

-   Set FAR threshold for early warning alerts to once per day.

-   Identify early-warning events using the ``EARLY_WARNING`` label rather than
    the ``EarlyWarning`` search type. The search type is already used to
    distinguish mock (``MDC``) events, so it cannot also be used to indicate
    early-warning events.

-   Inhibit GCNs for superevents with the INJ label.

-   Add configuration variable to disable all but MDC alerts from GCN, and
    set that variable to True on the production instance.

-   Skip the preliminary alert timeout for early warning events.

-   Update the documentation on RAVEN functions and external triggers flow
    chart.

-   Change BAYESTAR low frequency cutoff from 30 Hz (the default value) to
    15 Hz.

-   Change playground configuration to read O3 replay data rather than O2
    replay data.

-   Drop dependency on seaborn.

-   Defer sleekxmpp imports until the VOEvent client starts. This way,
    sleekxmpp is only imported in the thread that actually uses it. This
    should speed up worker startup by about 0.1 seconds.

-   Defer Comet and Twisted imports until they are actually needed by the
    VOEvent broker. This should speed up worker startup by about 0.2 seconds.

-   Defer imapclient imports until the email client starts. This should speed
    up worker startup by about 0.1 seconds.

-   Improve robustness of the email connection by resetting IMAP IDLE mode at
    least every 5 minutes and improving error-checking upon disconnection.

-   Add platform and hostname information to the Flask dashboard.

-   Ensure external sky maps are normalized.

0.12.3 (2020-03-24)
-------------------

-   Fix a bug that was introduced in GWCelery 0.12.1 that broke BAYESTAR
    localizations for PyCBC events. In 0.12.1, the BAYESTAR data handling
    was changed to merge together the contents of the coinc.xml and psd.xml.gz
    files into a single XML document so that BAYESTAR was not sensitive to the
    order in which the two files were passed to it. PyCBC includes the PSD data
    in its initial upload, and so its psd.xml.gz file is just a copy of
    coinc.xml. Merging the two documents together resulted in a single file
    with two copies of every LIGO-LW table, which broke subsequent parsing.

    Fix this by adding a special case for PyCBC to download the coinc.xml file
    only. This has the nice side effect of reducing the latency for PyCBC
    events because it is no longer necessary to wait for the additional GraceDB
    REST API calls involved in uploading and download the additional file.

0.12.2 (2020-03-20)
-------------------

-   Skip detchar checks for events which occur in the future.

-   Delay omegascans until data are available for events in the future.

-   Enable Zstandard compression of tasks and results to reduce bandwidth into
    and out of Redis.

-   Enable receipt confirmation of early warning GCN notices.

-   If available, use spatial coincidence FAR to determine when to publish a
    coincident event. Update both time and spatial FAR within superevent
    when publishable.

-   Fix bug where the superevent handler can trigger on external events.

0.12.1 (2020-03-12)
-------------------

-   Set delay to produce preliminary alert to 0 seconds in the playground
    configuration. In the production configuration, the delay is still 30
    seconds.

-   Adjust broker transport and worker settings so that the superevent worker
    respects task priorities. This is seen to reduce the latency of preliminary
    alerts by about 10 seconds.

-   The ``gwcelery.tasks.bayestar.localize`` task no longer cares about the
    order in which the ``coinc.xml`` and ``psd.xml.gz`` file contents arguments
    are passed to it because the task now combines the XML documents using
    ``ligolw_add``. This allows us to change the immediately upstream task in
    the localization canvas from an ``ordered_group`` to a ``group``. This
    avoids extra trips of the large file contents blobs into and out of Redis.
    This is seen to reduce the latency of the localization by about a second.

-   Produce GCN notices of type ``LVC_EARLY_WARNING`` for events that have the
    ``EarlyWarning`` search tag.

-   Add a new configuration variable ``early_warning_alert_far_threshold`` to
    control the FAR threshold for early warning alerts. In the playground
    environment, its value is the same as the threshold for ordinary CBC
    events. In the playground environment, its value is infinity, to generate
    alerts for all early warning events.

-   Fix bug where a SubGRBTargeted event would trigger a search in both Fermi
    and Swift.

0.12.0 (2020-03-05)
-------------------

-   Add the ``GCN_PRELIM_SENT`` label after the GCN notice has been sent.
    Previously, the ``GCN_PRELIM_SENT`` label was added after the GCN notice
    had been sent *and* after the GCN Circular template had been created. Since
    it takes many tens of seconds to create the GCN Circular template, this was
    distorting latency statistics.

-   Prioritize processing of ``label_added`` LVAlert messages over ``new``
    LVAlert messages in the superevent manager. The labels ``SKYMAP_READY``,
    ``EMBRIGHT_READY``, and ``PASTRO_READY`` must all be present before we can
    send a public alert, so processing ``label_added`` messages with higher
    priority may speed up preliminary alerts.

-   Increase the minimum concurrency of the main GWCelery worker pool from 4 to
    8 subprocesses in order to decrease latency.

-   Append to, and do not overwrite, log files, when starting GWCelery via
    Condor.

-   Launch raven coincidence search for sub-threshold GRBs separately for
    different gamma-ray experiments in order to use different time windows.
    This enables the joint LVK-Fermi and LVK-Swift targeted searches to be
    integrated with RAVEN.

-   Grab subGRB Fermi sky maps from GCN.

-   Create external sky maps for offline subGRBTargeted Swift uploads.

0.11.2 (2020-02-28)
-------------------

-   Document recommended value for the Redis server setting
    ``client-output-buffer-limit`` in order to prevent disconnection of Celery
    workers returning large task results. This value was established early in
    O3, but since it was not in the documentation, we frequently forgot to set
    it when configuring a Redis server on a new or upgraded system.

-   Add the unit test for tasks/inference.py.

-   Upload LALInference DAG files to save the exact commands run for the
    parameter estimation.

-   Fix the file names of Bayeswave PSDs.

-   Capture an exception that is produced when attempting to make an omega scan
    of data that contains NaNs.

-   Catch missing trigger_duration when launching check vectors for external
    events.

-   Run unit tests for Python 3.8.

-   Update ligo-followup-advocate to 1.1.6.

-   Update gracedb-sdk to 0.1.4.

-   When a GRB or SNEWS GCN is received, upload it to GraceDB with the correct
    group depending on the value of the VOEvent ``role`` attribute: if
    ``role="test"``, then upload to the ``Test`` group; if
    ``role="observation"``, then upload to the ``External`` group.

0.11.1 (2020-02-21)
-------------------

-   Un-pin LALSuite and use the latest stable version (at this time, 6.68).

-   Do not use Online_PE condor slots for lalinference parameter estimation.

0.11.0 (2020-02-21)
-------------------

-   Use Online_PE condor slots for lalinference parameter estimation.

-   Use Bayeswave PSD for online PE.

-   Fix a bug in skymap generation with online PE posterior samples.

-   Reduce the number of bilby runs for test events to less than once per day.

-   Add systematic error contributions to Fermi-GBM sky maps.

-   Convert Swift-BAT error radii from 90% C.L. to 1-sigma.

-   Add INTEGRAL and AGILE MCAL to GRB pipelines.

-   Apply label ``NOT_GRB`` to external Fermi candidates that do not meet
    required threshold of a GRB. This is determined by ``Most_Likely_Index``
    and ``Most_Likely_Prob`` quantities supplied with Fermi notices.
    RAVEN will not consider external events labeled ``NOT_GRB``.

-   Automatically generate and upload a graphic showing the source property
    values by means of a bar chart.

-   Pin astropy < 4.0 to work around an issue with caching of downloaded data
    on the Caltech cluster. See https://github.com/astropy/astropy/issues/9970.

-   Switch from GraceDB REST API calls from gracedb-client to gracedb-sdk to
    gain increased transaction throughput due to HTTP connection pooling.

-   Remove ``vetted=True`` keyword argument for GraceDB API calls to produce
    VOEvents, because that argument was removed from the GraceDB server and
    client over a year ago.

0.10.0 (2020-02-07)
-------------------

-   Decrease the number of OpenMP workers from 40 to 16, now that gstlal is
    uploading a reduced number of events.

-   Add VOEvent broker and receiver configuration for playground environment in
    order to enable end-to-end testing of transmission to and receipt from GCN.

-   Fix a bug in the upload of bilby results.

-   Do not start parameter estimation for mock events uploaded to
    gracedb.ligo.org.

-   Calculate joint spatio-temporal FAR automatically for external coincidences.
    Create the combined skymap when both the GW and external skymaps are
    available.

-   Increase the number of retries, with incremental retry backoff, when fetching
    the Fermi sky map from HEASARC. This is because the Fermi skymap is typically
    uploaded tens of minutes after the GCN notice from Fermi.

0.9.2 (2020-01-07)
------------------

-   Update to Celery 4.4.0.

-   Add bullet charts for BAYESTAR coherence-versus-incoherence Bayes factors.
    The BAYESTAR log Bayes factor for coherence versus incoherence is stored in
    the FITS file header's `LOGBCI` field. For each FITS file that has this
    header field, make a bullet chart to compare the log Bayes factor to a
    standard table of threshold confidence levels from Kass & Raftery (1995).

-   Enable the RAVEN alert pipeline by having the superevent manager listen
    to the label RAVEN_ALERT.

-   Use RAVEN VOEvent if RAVEN_ALERT.

-   Generate emcoinc circular if RAVEN_ALERT instead of EM_COINC.

-   Increase both CBC and Burst trials factors by one due to enabling the
    RAVEN pipeline.

-   Refactor ``gwcelery.tasks.detchar.make_omegascan`` to reuse GWPy's own
    plotting functions, instead of using our own Matplotlib code. This fixes a
    bug that prevented ``make_omegascan`` from working with Astropy 4.0 or
    later.

-   Unpin Astropy version, now that ``make_omegascan`` works with the most
    recent version.

0.9.1 (2019-12-15)
------------------

-   Produce an ``ADVREQ`` notification as soon as there is an alert which meets
    the public alert threshold, regardless of whether its annotations are
    complete. As a result, follow-up advocates will usually receive
    notifications about 30 seconds earlier, and will receive notifications even
    if some of the annotations fail.

-   Increase the FAR threshold of online PE to the public alert threshold.

-   Update lalsuite to lalsuite==6.63 and unpin scipy.

-   Change RAVEN to grab sky map from superevent. Block joint FAR calculation
    for SNEWS coincidences.

-   Skip Virgo data when online PE is started on O2Replay data since its
    statevector cannot be read by gwpy.

-   Modify RAVEN to run on MDC events.

-   Restrict the ``superevents.process`` task to process only complete
    G events instead of running for all the superevent completeness labels.
    The behavior for running on the ``new`` type events remains unchanged.

0.9.0 (2019-11-23)
------------------

This is the initial release of GWCelery for O3b.

-   Changes related to configuration settings

    - Use the Redis server that is provided by the operating system (e.g. as a
      systemd unit) rather than starting our own Redis server. This prevents a
      race condition between the shutdown of Redis and the shutdown of the
      workers that caused the workers to hang on shutdown.

    - Update HTCondor accounting group from O2 to O3.

    - Increase throughput for sky localization tasks by offloading processing
      of the ``openmp`` Celery queue to 40 workers that are launched via
      HTCondor on specially configured cluster nodes.

    - Use mpich as the MPI runtime for parameter estimation.

    - Use different HTCondor accounting groups for Celery workers depending on
      whether GWCelery is running in the playground environment
      (``ligo.dev.o3.cbc.pe.bayestar``) or the production environment
      (``ligo.prod.o3.cbc.pe.bayestar``).

    - Drop support for Python 3.6 so that we can use the ``check_output`` keyword
      argument that was added to ``suprocess.run()`` in Python 3.7.

    - Pin gwpy to <= 0.15.0 since the updated gwpy fails to read Virgo's state
      vector.

    - Update ligo-followup-advocate version to 1.1.3.

-   Changes related to superevent/orchestrator design

    - Add event completeness to publishability criterion. All three of
      ``PASTRO_READY``, ``SKYMAP_READY``, and ``EMBRIGHT_READY`` will be used
      to evaluate event completeness for CBC events. Only the ``SKYMAP_READY``
      label will be used to evaluate completeness for burst events.

    - Use ``EM_Selected`` to freeze the preferred event of a superevent and
      launch a preliminary alert.

    - Make sub-threshold annotations independent of annotations for superevents
      which pass public alert threshold.

    - Prevent second preliminary to be sent in the event of any advocate action.
      Previously, this was only being prevented for ADVNO.

    - Make skymaps from parameter estimation public automatically.

-   Changes related to online parameter estimation

    - Move a comment attached to posterior samples to
      the corresponding skymap.

    - Add bilby online parameter inference workflow.

    - Fix approximant name used for automatic parameter estimation.

    - Start parameter estimation on mock events.

    - Add acceptance tests of parameter estimation.

    - Use nodes dedicated to online PE also for playground events so that the
      test runs do not get stuck due to the lack of resources.

    - Add spins in online PE on playground events so that embright probabilities
      are calculated based on the posterior samples without errors.

    - Remove skymap generation from PE DAG so that it will not be generated
      twice.

    - Notify which pe pipeline failed for the failure of pe condor jobs.

-   Changes related to external coincidences

    - Create RAVEN circular if EM_COINC label is applied to superevent.

    - Make coincidence FAR synchronous within RAVEN pipeline to fix race
      condition.

    - Remove redundant SNEWS handler key.

    - Remove generation of em_coinc circular when ``EM_COINC`` label is
      applied.

    - Apply EM_COINC to preferred event when coincidence passes RAVEN publishing
      conditions.

    - Attempt fetching and uploading Fermi skymap upon receinving GCN notice.

-   Changes related to skymap generation

    - Revert back to running BAYESTAR for all ``G`` events.

    - Pass the ``-j`` flag to ``ligo-skymap-from-samples`` to speed up skymap
      generation.

-   Changes related to automated data quality checks

    - Create omegascans for all detectors upon creation of new superevent.

    - Run ``check_vectors`` upon the creation of a superevent. This will
      allow subthreshold superevents to be annotated with ``DQOK`` or
      ``DQV`` label.

-   Changes to the Flask dashboard

    - Teach preliminary alert form in Flask dashboard to present a dropdown of
      events sorted by the preferred event criterion.

    - Incorporate update circular into flask app.

-   Other changes

    - Add a task to ``em_bright.py`` to compute and upload source properties
      upon the upload of ``LALInference.posterior_samples.hdf5``.

0.8.7 (2019-09-14)
------------------

-   Update ligo-raven version to 1.17.

0.8.5.1 (2019-09-04)
--------------------

This is a non-sequential bugfix release based on version 0.8.5.1 to fix the
following issue:

-   Fix a lethal bug in ``em_bright.py`` introduced in version 0.8.5. The bug
    would incorrectly use the snr as the maximum mass of the NS and therefore
    the source property estimation for pipelines apart from gstlal would be
    grossly incorrect.

0.8.6 (2019-09-01)
------------------

-   Update ligo-raven version to 1.16.

-   Fix a bug that prevented retrying of failed GraceDB API calls in the
    superevent manager.

-   Add a retry for one more potential GraceDB API failure in the initial and
    update alert workflows.

-   In the playground environment only, upload each mock event several times in
    rapid succession with random jitter in order to simulate multiple pipeline
    uploads.

-   Expose events to the public prior to sending any kind of alert:
    preliminary, initial, update, or retraction. Previously this behavior only
    occurred for preliminary alerts, which created the unusual and undesirable
    possibility of a public GCN for an event that is not public. As before,
    events are only exposed to the public in the production environment, and
    not in the playground environment.

-   Propagate sky map file extensions (as in ``bayestar.fits.gz,1``) to the
    URLs that are presented in GCN notices.

-   Generate flattened FITS files and sky map visualizations for all
    superevents, even those that do not rise to the public alert threshold.
    Note that as a side effect all superevents will have the ``EM_Selected``
    label applied, since it is used as a semaphore to trigger the annotations.
    The ``ADVREQ`` label used to serve double duty as the semaphore and also as
    the wake-up call for follow-up advocates, but now it only serves the latter
    purpose.

    The feature of generating flattened FITS files and sky map plots for all
    superevents comes as a request from the Fermi and Swift sub-threshold
    searches.

-   Delay running BAYESTAR until the superevent's preferred event has
    stabilized. BAYESTAR is the most computationally intensive postprocessing
    task and running it for all events belonging to a superevent was a
    bottleneck.

-   For the playground environment only, decrease the timeout for stabilization
    of the preferred event from 5 minutes to 2 minutes, which is comparable to
    how long it has taken recent events to settle. This does not affect the
    configuration of the production environment.

-   Changed ``handle_cbc_event`` handler to call gstlal trained ML based
    inference for source property computation for gstlal triggers.

-   Apply EM_COINC to superevent and external event in parallel.

0.8.5 (2019-08-23)
------------------

-   Made raven.py tests more robust and have increased coverage.

-   Removed the feature of p-astro and em_bright reading mean counts,
    livetimes or ML classifiers from emfollow/data; moved them to
    lscsoft/p_astro as package data. Added back the ``test_tasks_p_astro.py``
    that was accidentally taken out in ``v0.8.0``. Pinned ``p_astro == 0.8.0``.

0.8.4 (2019-08-16)
------------------

-   Fixed a bug in ``gracedb.create_tag`` to handle the scenario when multiple
    log messages exist for the same filename. The tag is applied to the most
    recent log message.

-   Retry GraceDB API calls that fail due to receiving incomplete or malformed
    HTTP responses, as indicated by ``http.client.HTTPException`` exceptions.
    This should work around the increased incidence of ``RemoteDisconnected``
    exceptions that we have seen recently, and that caused a delay in sending
    out the preliminary alert for S190814bv.

0.8.3 (2019-08-09)
------------------

-   Enable Redis integration for Sentry error reporting.

-   Downgrade lalsuite to 6.54 since ``lalinference_pipe`` in 6.59 has a minor
    bug, which breaks automatic parameter estimation.

-   Include the number of participating detectors in the preferred event
    selection criterion for compact binaries: 3-detector events should be
    preferred over 2-detector events, and 2-detector events over 1-detector
    events, on the basis of more accurate localization. Ties are still broken
    by SNR.

-   Catch ``SystemExit`` exceptions from Python command line tools called in
    Celery tasks and re-raise them as ``RuntimeError`` exceptions so that they
    do not cause the workers to exit.

0.8.2 (2019-08-02)
------------------

-   Apply the ``public`` tag to data products before sending out an update GCN
    notice. This will prevent human errors related to not exposing LALInference
    files before sending a GCN notice.

-   Don't read the entire HTTP response from GraceDB POST requests. We only
    need the HTTP status code. This change might speed up GraceDB API calls a
    little bit.

-   Increase preliminary alert timeout back to 5 minutes.

-   Make ``gracedb.create_superevent``, ``gracedb.update_superevent`` and
    ``gracedb.add_event_to_superevent`` idempotent by catching the ``HTTPError``
    from GraceDB that occurs if the superevent has already been created.

-   Fix bug where neither the space-time nor temporal coincidence far is
    calculated if external sky map is unavailable.

-   Update ligo.skymap to 0.1.9. This version changes the data type of the
    multi-resolution HEALPix format's ``UNIQ`` column from an unsigned integer
    to a signed integer.

    Starting with this version, the Linux builds of ligo.skymap are compiled
    and optimized using the Intel C Compiler.

-   Change the trials factor for CBC searches to 4, since SPIIR is performing
    a single search, and that for burst to 3, since oLIB is not currently in
    operation.

0.8.1 (2019-07-29)
------------------

-   Downgrade lalsuite to 6.59.

-   Revert change that tried to fix incorrect key for querying external
    events. The keys were correct before.

0.8.0 (2019-07-26)
------------------

-   Assign ``gwcelery.tasks.skymaps.plot_volume`` tasks a reduced Celery
    priority as compared to ``gwcelery.tasks.bayestar.localize`` so that the
    latter are given preference. This ought to speed up the preparation of
    preliminary GCN notices because only the latter are required for GCNs but
    both kinds of tasks compete for slots in the resource-intensive OpenMP
    queue.

-   Reduce priority for CBC annotation tasks for events that do not pass the
    public alert threshold.

-   Update lalsuite to 6.60.

-   Ensure gracedb calls to create and update superevents are retried in
    the event of transient GraceDB API errors.

-   Update ligo-raven version to 1.15. Apply EM_COINC label in raven.py to
    give more control and prevent race conditions.

-   Use the space-time coincidence FAR as the default for RAVEN, use the
    temporal coincidence FAR when sky maps are not available.

-   Check if GRB is sub-threshold, set search to be 'SubGRB'. Pass search
    through external triggers pipeline and RAVEN.

-   Tune Celery's ``result_expires`` setting from its default value of one day
    to five minutes. Since we pass large byte strings as task arguments and
    return values, one day is too long to keep task tombstones in the database.
    This adjustment should reduce the memory footprint of the Redis server
    during periods with very high rates of GraceDB uploads.

    The downside is that task details will remain browsable in Flower for a
    much shorter period.

-   Remove p_astro_gstlal.py module, corresponding test modules, and
    documentation; p_astro will be reported as a pipeline product from gstlal.
    The computation of p_astro for all other pipelines is unaffected.

-   Fix EM_COINC bug where it is being over-applied to superevents.

-   Fix bug where wrong key was called for querying external events.

0.7.1 (2019-07-12)
------------------

-   The initial alert workflow will now consider only ``*.fits.gz`` sky maps
    and not ``*.fits`` sky maps for GCN Notices. It was an oversight that we
    did not exclude ``*.fits`` files from the list of extensions to consider
    when we updated the handling of multiresolution sky maps.

-   Catch and retry HTTP 429 ("Too Many Requests") errors from GraceDB.

-   Enable Sentry integration for Tornado in order to capture errors from the
    Flower console.

-   Fix file extensions for LALInference sky map PNG files: they should be
    named ``LALInference.png``, not ``LALInference.multiorder.png``.

-   Increase the Redis server's log verbosity in order to help diagnose Redis
    client connection dropouts.

-   Run sky map plotting and annotation tasks asynchronously so that they do
    not block sending preliminary alerts. Their outputs are only for human
    consumption; they are not needed in order to prepare GCN Notices.

0.7.0 (2019-06-21)
------------------

-   Trigger a preliminary alert for a superevent upon the first time that the
    preferred event is set to an event that meets the public alert criterion.

    This fixes a longstanding issue that has prevented automated preliminary
    alerts from being sent so far. The preferred event *at the instant that the
    timeout ended* did not meet the public alert criterion, but a preferred
    event that was selected some tens of seconds later did.

-   Decrease preliminary alert timeout to one minute.

-   The combined effect of these changes should be to decrease the latency for
    producing preliminary alerts from 7 minutes to 2 minutes.

0.6.3 (2019-06-14)
------------------

-   Work around a Celery canvas bug that prevented LALInference postprocessing
    from completing.

-   Fix a copy-paste error that caused ``DQV`` and ``INJ`` labels to be ignored
    when determining whether to send a preliminary alert.

-   Move RAVEN time coincidence windows to the application configuration.

-   Document the acceptence tests checklist in the instructions for preparing a
    release.

-   Update ligo-raven to version 1.14.

0.6.2 (2019-06-07)
------------------

-   Add a dependency on dnspython to silence the following warning message from
    SleekXMPP::

        DNS: dnspython not found. Can not use SRV lookup.

-   Pin some recently updated dependencies of Celery that caused unit test
    failures: amqp <= 2.4.2, kombu <= 4.5.0, vine <= 1.3.0.

-   Prevent subthreshold GRBs with low reliability from being processed as
    external events.

-   Add a task in orchestrator.py to generate FITS files and sky map images
    automatically whenever an HDF5 posterior samples file is uploaded.

-   Remove special-case handling of single-instrument events. Now, the
    eligibility of an event for a public alert is determined only on the basis
    of its false alarm rate.

-   Run parameter estimation on nodes dedicted to online-PE.

-   Emcoinc circular is triggered when RAVEN uploads a coincident FAR.

-   Pin scipy since scipy>=1.3.0 removes an interpolation function which
    lalinference postprocessing requires.

0.6.1 (2019-05-24)
------------------

-   Work around a bug in the Sentry Python SDK that caused excessive reporting
    of certain GraceDB exceptions that are listed in tasks' ``autoretry_for``
    settings. See `getsentry/sentry-python#370`_.

    ..  _`getsentry/sentry-python#370`: https://github.com/getsentry/sentry-python/issues/370

-   Change the name of BAYESTAR localization files to
    ``bayestar.multiorder.fits`` to distinguish them from flat-resolution
    HEALPix files, which are still named ``bayestar.fits.gz``.

-   Reimplement LVAlert listener as a Celery bootstep to avoid needing to track
    a singleton task using a Redis lock, because Redis locks do not play nicely
    with Redis persistence. The ``--lvalert`` command line option must now be
    passed in order to enable the LVAlert listener.

-   Turn on Redis database persistence so that Celery task state is preserved
    across restarts.

-   Add ``expose_to_public`` setting to disable exposing GraceDB events to the
    public in all environments except for production.

-   Update to the latest version of GWPy and un-pin Matplotlib because GWPy
    now supports Matplotlib 3.1.

-   Pin LALSuite to version 6.54 because LALInference in LALSuite 6.55 is not
    compatible with Python 3.

0.6.0 (2019-05-20)
------------------

-   Work around a bug in complex Celery canvases (see `celery/celery#5512`_)
    that prevented initial GCN notices from being sent. As a side effect of
    this workaround, the initial, update, and retraction canvases will not
    automatically expose events to the public.

    The preliminary alert canvas still *does* expose events to the public, so
    under normal circumstances, the follow-up advocate should not have to
    manually do that. However, if the event has not been exposed to the public
    for whatever reason, then the follow-up advocate should expose it to the
    public manually before applying the ``ADVOK`` label.
    See `emfollow/followup-advocate-guide!2`_.

    ..  _`celery/celery#5512`: https://github.com/celery/celery/issues/5512
    ..  _`emfollow/followup-advocate-guide!2`: https://git.ligo.org/emfollow/followup-advocate-guide/merge_requests/2

-   Reduce the false alarm rate threshold for parameter estimation to decrease
    cluster load.

-   Remove redundant LVAlert subscription in handle_lvalert_grb to prevent
    double calls to RAVEN.

-   Read template weights for P_astro from hdf5 file using h5py for speedup.

-   Require matplotlib < 3.1 becuase matplotlib 3.1 breaks importing gwpy::

        /usr/local/lib/python3.7/site-packages/gwpy/plot/rc.py:79: in <module>
            rcParams.get('text.latex.preamble', []) + tex.MACROS),
        E   TypeError: can only concatenate str (not "list") to str

-   Make ``gwcelery.tasks.gracedb.get_superevents`` and
    ``gwcelery.tasks.gracedb.get_events`` take any number of keyword arguments
    to be passed to corresponding client methods.

-   Update the superevent ``t_0`` field whenever the preferred event changes.

0.5.7 (2019-05-13)
------------------

-   If the VOEvent broker is disabled by setting ``voevent_broker_whitelist``
    to an empty list, then suppress the normal error message that would occur
    when attempting to send a VOEvent when there are no broker connections.

-   Rearrange preliminary alert workflow so that sky map plots are generated
    for the newly added FITS file rather than an older FITS file that
    coincidentally has the same name.

-   Have ``gwcelery.detchar.check_vectors`` task apply all GraceDB log messages
    in order to increase robustness to recoverable GraceDB API errors.

-   Port over majority of P_astro code from gwcelery to the p-astro package.

-   Use cleaned data for parameter estimation.

-   The ``DQOK`` and ``DQV`` labels should be mutually exclusive. When
    ``gwcelery.tasks.detchar.check_vectors`` adds one of the ``DQOK`` or
    ``DQV`` labels, it will now first remove the other label.

-   Change exception in VOEevent parsing of Fermi subtreshold alerts to
    match real incoming alerts.

-   Update Celery to 4.3.0.

-   Automatically select the most up-to-date calibration uncertainties for
    parameter estimation.

0.5.6 (2018-05-08)
------------------

-   Extend the ``orchestrator_timeout`` to 300s and the ``pe_timeout`` to
    345s. The previous timeout was not sufficient for the online pipelines
    to upload all of their possible candidates, hence the extension.

0.5.5 (2019-05-03)
------------------

-   Cycle through llhoft, high latency frames, and low latency frames in
    detchar's cache creation.

-   Add explanations on options in online_pe.jinja2 for those who start
    parameter estimation based on the ini files uploaded to GraceDB.

-   Calculate horizon distance with psd.xml.gz to determine the upper limit of
    distance prior for parameter estimation.

-   Start parameter estimation when the lowest FAR of the events in a
    superevent is lower than the threshold.

-   Update the calibration uncertainties used for parameter estimation.

-   Handle an exception in VOEvent parsing of Fermi subthreshold alerts due to
    different param names.

-   Stop uploading corner plots of intrinsic parameters.

-   Connect to different GCN servers to receive alerts in the production and
    playground environments, because GCN does not support multiple receiver
    connections from the same client IP address to the same server.

-   Change the preferred event assignment logic to not let accidental candidates
    like G330298 which have low FAR but high SNR values to become the preferred
    event. From now on, ``superevents.should_publish`` takes maximum precedence
    for selecting the preferred event. The same is also used by orchestrator
    to expose events.

-   Update RAVEN coinc FAR task call which uses string params versus
    un-pickleable class object params.

-   Make sure to consume the entire response from every GraceDB API request.
    This will ensure that GraceDB API call has completed before the pipeline
    continues, and will fix errors like we encountered with S190426c where
    the pipeline would march along before uploads had finished.

-   Apply ADVREQ label earlier in the preliminary alert workflow.

-   Update LALSuite to version 6.54. We are now using a stable version again
    instead of a nightly build.

-   Add Nagios checks for GCN connectivity.

-   Improve uploaded comments so that it is easily understood which event has
    triggered parameter estimation.

0.5.4 (2019-05-01)
------------------

-   Provide a value for terrestrial count for P_astro for non-gstlal
    pipelines that is consistent with the FAR threshold used.

0.5.3 (2019-04-17)
------------------

-   Update ligo-followup-advocate to 0.0.28.

-   Stop using unreviewed cleaned data for parameter estimation.

-   Update detchar check to analyze full template duration for CBC events.

0.5.2 (2019-04-15)
------------------

-   Fix typo in ``gracedb.get_instruments``: there was the attribute lookup
    ``single.ifo``, which should have been the dictionary lookup
    ``single[ifo]``.

-   Fix ``gwcelery.tasks.p_astro_other.choose_snr`` for gstlal. This method did
    not previously expect to be called for gstlal, since it is typically only
    invoked for other pipelines. However, there is one case when ``choose_snr``
    is invoked for gstlal, which is when the ranking_data file from gstlal is
    corrupted with NaNs, causing P_astro for gstlal to fail. Thus, choose_snr
    has now been fixed to also handle gstlal as a pipeline.

0.5.1 (2019-04-12)
------------------

-   Changed default for em-bright from 2.83 to 3.0 M_sun to be consistent with
    notices.

0.5.0 (2019-04-12)
------------------

-   Give permissions to read the files under parameter estimation run
    directories to non-owner people so that rota people can check their
    progresses. The naming convention of the run directories changed.

-   EM-Bright ML classification requires review. Until then, give answer based
    on low-latency estimates.

-   Compute P_astro with mass-based template weighting. Template weights are
    now keyed on template parameters, rather than bin numbers. This should make
    P_astro immune to binning conventions.

-   Add form to manually send a preliminary GCN Notice.

-   Fix a typo in ``gwcelery.sub`` that caused the Flower dashboard to fail to
    start.

-   Round iDQ p(glitch) to 3 decimal places in GraceDB log message.

-   Switch log telemetry from the on-premise instance of Sentry at Caltech to a
    cloud-hosted subscription to sentry.io.

-   In the playground configuration, the ``gwcelery.tasks.gcn.validate`` task
    was producing false alarms because the GCN receiver was receiving VOEvents
    from the production instance, which would certainly differ in content from
    VOEvents in the playground instance. Fix this by having
    ``gwcelery.tasks.gcn.validate`` discard all VOEvents if the VOEvent
    broadcaster is disabled.

-   Update ligo-followup-advocate to 0.0.27.

-   Wait for 1 minute before parameter estimation in case the preferred event
    is updated with high latency.

-   Ensure that P_astro accounts for very loud MBTA and PyCBC events, whose FAR
    saturate at certain low values depending on instrument combination, but
    whose SNRs can increase indefinitely.

-   When a user triggers a Preliminary or Update alert through the Flask
    interface, create a GraceDB log message to record the username.

-   The Flask interface will now show a confirmation dialog before sending any
    alerts.

-   Add a terrifying warning to the Flask interface to make it clear that the
    interface is live.

0.4.3 (2019-04-05)
------------------

-   Now that LIGO/Virgo alerts are public, switch the GCN listener that we use
    to confirm receipt of our own GCN Notices from a managed, private
    connection to an anonymous, public connection.

-   Migrate the Flask and Flower dashboards from ldas-jobs.ligo.caltech.edu to
    emfollow.ligo.caltech.edu. The new URLs are:

    *   https://emfollow.ligo.caltech.edu/gwcelery
    *   https://emfollow.ligo.caltech.edu/flower
    *   https://emfollow.ligo.caltech.edu/playground/gwcelery
    *   https://emfollow.ligo.caltech.edu/playground/flower

    Remove the htaccess file from our public_html directory, since the reverse
    proxy configuration is now the responsibility of system administrators.

-   Display the GWCelery version number in the Flask application.

-   Add visualizations for ``p_astro.json`` source classification files.

0.4.2 (2019-04-05)
------------------

-   Calculation of number of instruments is now unified across superevent
    manager and orchestrator using gracedb method ``get_number_of_instruments``.

-   Enable automated preliminary alerts for all pipelines because disabling
    them in the orchestrator introduced some issues due to the criteria for
    releasing a public alert drifting away from the definition of a the
    preferred event of a superevent. We will instead trust pipelines that are
    still under review will upload events to the playground rather than the
    production environment.

0.4.1 (2019-04-02)
------------------

-   Fixed normalization issues with p_astro_gstlal.py; normalization
    was being applied in the wrong places during Bayes factor
    computation.

-   Require celery < 4.3.0 because that version breaks the nagios unit tests.

-   Update false alarm rate trials factors for preliminary alerts.

-   Enable sending GCN notices for fully automated preliminary alerts.

-   Add threshold_snr option in online_pe.jinja2, which is used to determine
    the upper limit of distance prior.

-   Use the same criteria to decide whether to expose an event publicly in
    GraceDB as we use to decide whether to issue a public alert.

-   Do not issue public alerts for single-instrument GW events.

-   Disable automated preliminary alerts for all pipelines but gstlal and cWB
    due to outstanding review items for the other pipelines.

0.4.0 (2019-03-29)
------------------

-   This is the penultimate release before LIGO/Virgo observing run 3 (O3).

-   Make detchar results easier to read by formatting as HTML table.

-   Allow iDQ to label DQV onto events based on p(glitch). Adjustable by
    pipeline.

-   Move functions in tasks/lalinference.py to lalinference_pipe.py in
    lalsuite.

-   Take into account calibration errors in automatic Parameter Estimation.

-   Do not use margphi option for automatic Parameter Estimation with ROQ
    waveform since that option is not compatible with ROQ likelihood.

-   Adjust WSGI middleware configuration to adapt to a change in Werkzeug
    0.15.0 that broke redirects on form submission in the Flask app. See
    https://github.com/pallets/werkzeug/pull/1303.

-   Use the new ``ligo.lw`` module for reading gstlal's
    ``ranking_data.psd.xml.gz`` files, because these files are now written
    using the new LIGO-LW format that uses integer row IDs.

-   Use clean data for parameter estimation.

-   Use production accounting group for PE runs on gracedb events.

-   Change threshold from log-likelihood equals 6 to a dynamic threshold that
    ensures that all gstlal events uploaded to gracedb get assigned a P_astro
    value.

0.3.1 (2019-03-18)
------------------

-   Fix a bug in translating keys from ``source_classification.json`` to
    keyword arguments for ``GraceDB.createVOEvent`` that caused VOEvents to
    be missing the ``HasNS`` and ``HasRemnant`` fields.

-   FAR threshold for sending preliminary notices for CBC is changed to
    1 per 2 months.

-   Upload log files when LALInference parameter estimation jobs fail or are
    aborted.

-   Changed the filename ``source_classification.json`` to ``em_bright.json``.

-   Change condor log directory from /var/tmp to ~/.cache/condor since gwcelery
    workers have separate /var/tmp when they are running as condor jobs and
    that causes problems when gwcelery tries to read log files.

-   Limit the maximum version of gwpy to 0.14.0 in order to work around a unit
    test failure that started with gwpy 0.14.1. See
    https://git.ligo.org/emfollow/gwcelery/issues/95.

-   Upload a diff whenever a LIGO/Virgo VOEvent that we receive from GCN does
    not match the original that we sent.

-   Wait for low-latency or high-latency frame files being transferred to the
    cluster before parameter estimation starts.

0.3.0 (2019-03-01)
------------------

-   Fixed exponent in the expression of foreground count in p_astro_other task.

-   Run the sky map postprocessing and add the ``PE_READY`` tag when
    LALInference finishes.

-   Include ``EM_COINC`` triggered circulars to upload to the superevent page.

-   p-astro reads mean values from a file on CIT, new mass-gap category
    added. Removed redundant functions from p_astro_gstlal module.

-   Continuous deployment on the Caltech cluster now uses a robot keytab and
    ``gsissh`` instead of SSH keys and vanilla ``ssh`` because the new
    my.ligo.org SSH key management does not support scripted access.

-   Improve the isolation between the production and playground instances of
    GWCelery by deploying them under two separate user accounts on the Caltech
    cluster.

-   Add functionality for em_bright task to query ``emfollow/data``
    for trained machine learning classifier and report probabilities
    based on it.

0.2.6 (2019-02-12)
------------------

-   Report an environment tag to Sentry corresponding to the GWCelery
    configuration module (``production``, ``test``, ``playground``, or
    ``development``) in order to differentiate log messages from different
    deployments.

-   The ``gwcelery condor`` command now identifies jobs that it owns by
    matching both the job batch name and the working directory. This makes it
    possible to run multiple isolated instances of GWCelery under HTCondor on
    the same cluster in different working directories.

-   Change the conditions for starting parameter estimation. For every CBC
    superevent, create an ``online_pe.ini`` file suitable for starting
    LALInference. However, only start LALInference if the false alarm rate is
    less than once per 2 weeks.

-   Determine PSD segment length for LALInference automatically based on data
    availability and data quality.

-   Add a Flask-based web interface for manually triggering certain tasks such
    as sending updated GCN notices.

0.2.5 (2019-02-01)
------------------

-   Pass along the GWCelery version number to Sentry.

-   Upload stdout and stderr when dag creation fails and notifications when
    submitted job fails in Parameter Estimation

-   Allow detchar module's ``create_cache`` to use gwdatafind when frames
    are no longer in llhoft.

-   The Nagios monitoring plugin will now report on the status of LVAlert
    subscriptions.

-   Change trials factor to 5 for both CBC and Burst categories. CBC includes
    the 4 CBC pipelines. Burst includes the 4 searches performed in total by
    the 2 Burst pipelines. An additional external coincidence search.

-   Automatically set up PE ini file depending on source parameters
    reported by detection pipelines.

0.2.4 (2018-12-17)
------------------

-   Fix broken links in log messages due to changes in GraceDB URL routes.

-   Whenever we send a public VOEvent using GCN, also make the corresponding
    VOEvent file in GraceDB public.

-   Don't include Mollweide projection PNG file in VOEvents. The sky map
    visualizations take longer to generate than the FITS files themselves, so
    they were unnecessarily slowing down the preliminary alerts.

-   Preliminary GCN FAR threshold is modified to be group (CBC, Burst, Test)
    specific.

0.2.3 (2018-12-16)
------------------

-   Update frame type used in LALInference Parameter Estimation.

-   Handle cases where ``p_astro_gstlal.compute_p_astro`` returns NaNs by
    falling back to ``p_astro_other.compute_p_astro``.

-   Fix a bug that prevented annotations that are specific to 3D sky maps from
    being performed for multi-resolution FITS files.

-   Fetch the graceid for the new event added from the gracedb logs
    since superevent packet does not provide information as to which
    event is added in case of type event_added.

0.2.2 (2018-12-14)
------------------

-   Add error handling for nonexistent iDQ frames in detchar module.

0.2.1 (2018-12-14)
------------------

-   Update detchar module configuration for ER13.

0.2.0 (2018-12-14)
------------------

-   This is the release of GWCelery for ER13.

-   Run two separate instances of Comet, one to act as a broker and one to act
    as a client. This breaks a cycle that would cause retransmission of GRB
    notices back to GCN.

-   Fix a race condition that could cause preliminary alerts to be sent out for
    events for which data quality checks had failed.

-   Unpin the ``redis`` package version because recent updates to Kombu and
    Billiard seem to have fixed the Nagios unit tests.

-   Start the Comet VOEvent broker as a subprocess intead of using
    ``multiprocessing`` and go back to using PyGCN instead of Comet as the
    VOEvent client. This is a workaround for suspected instability due to a bad
    interaction between ``redis-py`` and ``multiprocessing``.

-   Reset Matplotlib's style before running ``ligo-skymap-plot`` and
    ``ligo-skymap-plot-volume``. There is some other module (probably in
    LALSuite) that is messing with the rcparams at module scope, which was
    causing Mollweide plots to come out with unusual aspect ratios.

-   Run ``check_vectors`` upon addition of an event to a superevent if the
    superevent already has an ``DQV`` label.

-   Do not check the DMT-DQ_VECTOR for pipelines which use gated h(t).

-   Remove static example VOEvents from the Open Alert Users Guide. We never
    used them because activating sample alerts got help until ER13.

-   Disable running the Orchestrator for test events for ER13. After ER13 is
    over, we need to carefully audit the code and make sure that test events
    are handled appropriately.

-   Enable public GraceDB entries and public GCNs for mock (MDC) events. For
    **real** events in ER13, disable public preliminary GCNs. Instead, advocate
    signoffs will trigger making events and GCN notices public: ``ADVOK`` for
    initial notices and ``ADVNO`` for retraction notices.

-   Include source classification output (BNS/NSBH/BBH/Terrestrial) in GCN
    Notices.

0.1.7 (2018-11-27)
------------------

-   Pin the ``redis`` package version at <3 because the latest version of redis
    breaks the Nagios unit tests.

-   Ditch our own homebrew VOEvent broker and use Comet instead.

-   In addition to traditional flat, fixed-nside sky maps, BAYESTAR will now
    also upload an experimental multiresolution format described in
    `LIGO-G1800186-v4 <https://dcc.ligo.org/LIGO-G1800186-v4/public>`_.

0.1.6 (2018-11-14)
------------------

-   Update URL for static example event.

0.1.5 (2018-11-13)
------------------

-   Add tasks for submitting HTCondor DAGs.

-   Add a new module, ``gwcelery.tasks.lalinference``, which provides tasks to
    start parameter estimation with LALInference and upload the results to
    GraceDB.

-   Depend on lalsuite nightly build from 2018-11-04 to pick up changes to
    LALInference for Python 3 support.

-   Send static example VOEvents from the Open Alert Users Guide.
    This will provide a stream of example alerts for astronomers until GraceDB
    is ready for public access.

-   Add trials factor correction to the event FAR when comparing against
    FAR threshold to send out preliminary GCN.

-   Require that LIGO/Virgo VOEvents that we receive from GCN match the
    original VOEvents from GraceDB byte-for-byte, since GCN will now pass
    through our VOEvents without modification.

0.1.4 (2018-10-29)
------------------

-   Work around a bug in astropy.visualization.wcsaxes that affected all-sky
    plots when Matplotlib's ``text.usetex`` rcparam is set to ``True``
    (https://github.com/astropy/astropy/issues/8004). This bug has evidently
    been present since at least astropy 1.3, but was not being triggered until
    recently: it is likely that some other package that we import
    (e.g. lalsuite) is now globally setting ``text.usetex`` to ``True``.

-   A try except is added around updateSuperevent to handle a bad
    request error from server side when updating superevent parameters
    which have nearby values.

-   Send automatic preliminary alerts only for events with a false alarm rate
    below a maximum value specified by a new configuration variable,
    ``preliminary_alert_far_threshold``.

-   State vector vetoes will not suppress processing of preliminary sky maps
    and source classification. They will still suppress sending preliminary
    alerts.

-   Set ``open_alert`` to ``True`` for all automated VOEvents.

0.1.3 (2018-10-26)
------------------

-   Preliminary GCN is not sent for superevents created from offline gw events.

-   Add ``dqr_json`` function to ``gwcelery.tasks.detchar``, which uploads a
    DQR-compatible json to GraceDB with the results of the detchar checks.

-   Depend on ligo.skymap >= 0.0.17.

-   Fix a bug in sending initial, update, and retraction GCN notices: we were
    sending the VOEvent filenames instead of the file contents.

0.1.2 (2018-10-11)
------------------

-   Setted ``vetted`` flag to true for all initial, update, and retraction
    alerts that are triggered by GraceDB signoffs.

-   Write GraceDB signoffs, instead of just labels, to simulate initial and
    retraction alerts for mock events, because merely creating the ``ADVNO``
    or ``ADVOK`` label does not cause GraceDB to erase the ``ADVREQ`` label.
    This change makes mock alerts more realistic.

-   Change filename of cWB sky maps from ``skyprobcc_cWB.fits`` to
    ``cWB.fits.gz`` for consistency with other pipelines.

-   Any time that we send a VOEvent, first change the GraceDB permissions on
    the corresponding superevent so that it is visible to the public. Note that
    this has no effect during the ongoing software engineering runs because
    LVEM and unauthenticated access are currently disabled in GraceDB.

0.1.1 (2018-10-04)
------------------

-   Use the ``public`` tag instead of the ``lvem`` tag to mark preliminary sky
    maps for public access rather than LV-EM partner access. Note that GraceDB
    has not yet actually implemented unauthenticated access, so this should
    have no effect during our ongoing software engineering runs.

-   Add ``check_idq`` function to detchar module, which reads probabilities
    generated by iDQ.

-   Automated ``DQV`` labels should not trigger retraction notices because they
    prevent preliminary notices from being sent in the first place.

-   The criterion for selecting a superevent's preferred event now prefers
    multiple-detector events to single-detector events, with precedence over
    source type (CBC versus burst). Any remaining tie is broken by using SNR
    for CBC and FAR for Burst triggers.

-   By default, initial and update alerts will find and send the most recently
    added public sky map.

-   The initial and update sky maps no longer perform sky map annotations,
    because they would only be duplicating the annotations performed as part
    of the preliminary alert.

-   Mock events now include example initial and retraction notices. Two minutes
    after each mock event is uploaded, there will be either an ``ADVOK`` or an
    ``ADVNO`` label applied at random, triggering either an initial or a
    retraction notice respectively.

-   Depend on ligo-gracedb >= 2.0.1 in order to pull in a bug fix for VOEvents
    with ProbHasNS or ProbHasRemnant set to 0.0.

-   Use the ``sentry-sdk`` package instead of the deprecated ``raven`` package
    for Sentry integration.

0.1.0 (2018-09-26)
------------------

-   Separated the external GCN listening handlers into two: one that listens
    to GCNs about SNEWS triggers and another that listens to Fermi and Swift.

-   Fixed calls to the raven temporal coincidence search so that search results
    separate SNEWS triggers from Fermi and Swift.

-   Add space-time FAR calculation for GRB and GW superevent coincidences.
    This only runs when skymaps from both triggers are available to download.

-   Add human vetting for initial GCN notices. For each new superevent that
    passes state vector checks, the ``ADVREQ`` label is applied. Rapid response
    team users should set their GraceDB notification preferences to alert
    them on ``ADVREQ`` labels. If a user sets the ``ADVOK`` label, then an
    initial notice is issued. If a user sets the ``ADVNO`` label, then a
    retraction notice is issued.

-   Update the LVAlert host for gracedb-playground.ligo.org.

-   Add experimental integration with `Sentry <https://sentry.io/>`_ for log
    aggregation and error reporting.

-   Track API and LVAlert schema changes in ligo-gracedb 2.0.0.

0.0.31 (2018-09-04)
-------------------

-   Refactor external trigger handling to separate it from the orchestrator.

-   Fixed a bug in the VOEvent broker to only issue "iamalive" messages after
    sending the first VOEvent.

-   Pass group argument to set time windows appropriately when performing raven
    coincidence searches. Search in the [-600, 60]s range and [-5, 1]s range
    around external triggers for Burst events and CBC events respectively.
    Similarly, search in the [-60, 600]s and [-1, 5]s range around Burst and
    CBC events for external triggers.

-   Compute and upload FAR for GRB external trigger/superevent coincidence upon
    receipt of the EM_COINC label application to a superevent.

-   Add continuous integration testing for Python 3.7, and run test suite
    against all supported Python versions (3.6, 3.7).

-   Update ligo.skymap to 0.0.15.

0.0.30 (2018-08-02)
-------------------

-   Manage superevents for production, test, and MDC events separately.

-   Add some more validation of LIGO/Virgo VOEvents from GCN.

-   Remove now-unused task ``gwcelery.tasks.orchestartor.continue_if``.

-   Add ``check_vectors`` run for external triggers.

-   Change the preferred event selection criteria for burst events
    to be FAR instead of SNR.

-   Add ``gwcelery nagios`` subcommand for Nagios monitoring.

-   Incorporate Virgo DQ veto streams into ``check_vectors``

-   Update ligo-raven to 1.3 and ligo-followup-advocate to 0.0.11.

0.0.29 (2018-07-31)
-------------------

-   Add a workflow graph to superevents module documentation.

-   Add ``gwcelery condor resubmit`` as a shortcut for
    ``gwcelery condor rm; gwcelery condor submit``.

-   Fix deprecation warning due to renaming of
    ``ligo.gracedb.rest.Gracedb.createTag`` to
    ``ligo.gracedb.rest.Gracedb.addTag``.

-   Update ligo-gracedb to 2.0.0.dev1.

0.0.28 (2018-07-25)
-------------------

-   Add injection checks to ``check_vector``.

-   Bitmasks are now defined symbolically in ``detchar``.

-   Refactor configuration so that it is possible to customize settings
    through an environment variable.

0.0.27 (2018-07-22)
-------------------

-   The preferred event for superevents is now decided based on higher SNR
    value instead of lower FAR in the case of a tie between groups.

-   A check for the existence of the gstlal trigger database is performed
    so that compute_p_astro does not return None.

0.0.26 (2018-07-20)
-------------------

-   Fix spelling of the label that is applied to events after p_astro finishes,
    changed from ``P_ASTRO_READY`` to ``PASTRO_READY``.

-   Run p_astro calculation for mock events.

-   Overhaul preliminary alert pipeline so that it is mostly feature complete
    for both CBC and Burst events, and uses a common code path for both types.
    Sky map annotations now occur for both CBC and Burst localizations.

-   Switch to using the pre-registered port 8096 for receiving proprietary
    LIGO/Virgo alerts on emfollow.ligo.caltech.edu. This means that the
    capability to receive GCNs requires setting up a site configuration in
    advance with Scott Barthelmey.

    Once we switch to sending public alerts exclusively, then we can switch
    back to using port 8099 for anonymous access, requiring no prior site
    configuration.

0.0.25 (2018-07-19)
-------------------

-   Reintroduce pipeline-dependent pre/post peeks for ``check_vector`` after
    fixing issue where pipeline information was being looked for in the wrong
    dictionary.

-   ``check_vector`` checks all detectors regardless of instruments used, but
    only appends labels based on active instruments.

-   Fix a few issues in the GCN broker:

    *   Decrease the frequency of keepalive ("iamalive" in VOEvent Transport
        Protocol parlance) packets from once a second to once a minute at the
        request of Scott Barthelmey.

    *   Fix a possible race condition that might have caused queued VOEvents to
        be thrown away unsent shortly after a scheduled keepalive packet.

    *   Consume and ignore all keepalive and ack packets from the client so
        that the receive buffer does not overrun.

-   Add ``p_astro`` computation for ``gstlal`` pipeline. The copmutation is
    launched for all cbc_gstlal triggers.

0.0.24 (2018-07-18)
-------------------

-   Revert pipeline-dependent pre/post peeks for ``check_vector`` because they
    introduced a regression: it caused the orchestrator failed without running
    any annotations.

0.0.23 (2018-07-18)
-------------------

-   Add timeout and keepalive messages to GCN broker.

-   Update ligo-gracedb to 2.0.0.dev0 and ligo.skymap to 0.0.12.

-   Add superevent duration for gstlal-spiir pipeline.

-   Fix fallback for determining superevent duration for unknown pipelines.

-   Make ``check_vector`` pre/post peeks pipeline dependent.

0.0.22 (2018-07-11)
-------------------

-   Process gstlal-spiir events.

-   Create combined LVC-Fermi skymap in case of coincident triggers and
    upload to GraceDB superevent page. Also upload the original external
    trigger sky map to the external trigger GraceDB page.

-   Generalize conditional processing of complex canvases by replacing the
    ``continue_if_group_is()`` task with a more general task that can be used
    like ``continue_if(group='CBC')``.

-   Add a ``check_vector_prepost`` configuration variable to control how much
    padding is added around an event for querying the state vector time series.

    This should have the beneficial side effect of fixing some crashes for
    burst events, for which the bare duration of the superevent segment was
    less than one sample.

0.0.21 (2018-07-10)
-------------------

-   MBTA events in GraceDB leave the ``search`` field blank. Work around this
    in ``gwcelery.tasks.detchar.check_vectors`` where we expected the field
    to be present.

-   Track change in GraceDB JSON response for VOEvent creation.

0.0.20 (2018-07-09)
-------------------

-   After fixing some minor bugs in code that had not yet been tested live,
    sending VOEvents to GCN now works.

0.0.19 (2018-07-09)
-------------------

-   Rewrite the GCN broker so that it does not require a dedicated worker.

-   Send VOEvents for preliminary alerts to GCN.

-   Only perform state vector checks for detectors that were online,
    according to the preferred event.

-   Exclude mock data challenge events from state vector checks.

0.0.18 (2018-07-06)
-------------------

-   Add detector state vector checks to the preliminary alert workflow.

0.0.17 (2018-07-05)
-------------------

-   Undo accidental configuration change in last version.

0.0.16 (2018-07-05)
-------------------

-   Stop listening for three unnecessary GCN notice types:
    ``SWIFT_BAT_ALARM_LONG``, ``SWIFT_BAT_ALARM_SHORT``, and
    ``SWIFT_BAT_KNOWN_SRC``.

-   Switch to `SleekXMPP <http://sleekxmpp.com>`_ for the LVAlert client,
    instead of `PyXMPP2 <http://jajcus.github.io/pyxmpp2/>`_. Because SleekXMPP
    has first-class support for publish-subscribe, the LVAlert listener can
    now automatically subscribe to all LVAlert nodes for which our code has
    handlers. Most of the client code now lives in a new external package,
    `sleek-lvalert <https://git.ligo.org/emfollow/sleek-lvalert>`_.

0.0.15 (2018-06-29)
-------------------

-   Change superevent threshold and mock event rate to once per hour.

-   Add ``gracedb.create_label`` task.

-   Always upload external triggers to the 'External' group.

-   Add rudimentary burst event workflow to orchestrator: it just generates
    VOEvents and circulars.

-   Create a label in GraceDB whenever ``em_bright`` or ``bayestar`` completes.

0.0.14 (2018-06-28)
-------------------

-   Fix typo that was causing a task to fail.

-   Decrease orchestrator timeout to 15 seconds.

0.0.13 (2018-06-28)
-------------------

-   Change FAR threshold for creation of superevents to 1 per day.

-   Update ligo-followup-advocate to >= 0.0.10. Re-enable automatic generation
    of GCN circulars.

-   Add "EM bright" classification. This is rudimentary and based only on the
    point mass estimates from the search pipeline because some of the EM bright
    classifier's dependencies are not yet ready for Python 3.

-   Added logic to select CBC events as preferred event over Burst. FAR acts
    as tie breaker when groups for preferred event and new event match.

-   BAYESTAR now adds GraceDB URLs of events to FITS headers.

0.0.12 (2018-06-28)
-------------------

-   Prevent receiving duplicate copies of LVAlert messages by unregistering
    redundant LVAlert message types.

-   Update to ligo-followup-advocate >= 0.0.9 to update GCN Circular text for
    superevents. Unfortunately, circulars are still disabled due to a
    regression in ligo-gracedb (see
    https://git.ligo.org/lscsoft/gracedb-client/issues/7).

-   Upload BAYESTAR sky maps and annotations to superevents.

-   Create (but do not send) preliminary VOEvents for all superevents.
    No vetting is performed yet.

0.0.11 (2018-06-27)
-------------------

-   Submit handler tasks to Celery as a single group.

-   Retry GraceDB tasks that raise a ``TimeoutError`` exception.

-   The superevent handler now skips LVAlert messages that do not affect
    the false alarm rate of an event (e.g. simple log messages).

    (Note that the false alarm rate in GraceDB is set by the initial event
    upload and can be updated by replacing the event; however replacing the
    event does not produce an LVAlert message at all, so there is no way to
    intercept it.)

-   Added a query kwarg to superevents method to reduce latency in
    fetching the superevents from gracedb.

-   Refactored getting event information for update type events so
    that gracedb is polled only once to get the information needed
    for superevent manager.

-   Renamed the ``set_preferred_event`` task in gracedb.py to
    ``update_superevent`` to be a full wrapper around the ``updateSuperevent``
    client function. Now it can be used to set preferred event and also update
    superevent time windows.

-   Many ``cwb`` (extra) attributes, which should be floating point
    numbers, are present in lvalert packet as strings. Casting them
    to avoid embarassing TypeErrors.

-   Reverted back the typecasting of far, gpstime into float. This is
    fixed in https://git.ligo.org/lscsoft/gracedb/issues/10

-   CBC ``t_start`` and ``t_end`` values are changed to 1 sec interval.

-   Added ligo-raven to run on external trigger and superevent creation
    lvalerts to search for coincidences. In case of coincidence, EM_COINC label
    is applied to the superevent and external trigger page and the external
    trigger is added to the list of em_events in superevent object dictionary.

-   ``cwb`` and ``lib`` nodes added to superevent handler.

-   Events are treated as finite segment window, initial superevent
    creation with preferred event window. Addition of events to
    superevents may change the superevent window and also the
    preferred event.

-   Change default GraceDB server to https://gracedb-playground.ligo.org/
    for open public alert challenge.

-   Update to ligo-gracedb >= 1.29dev1.

-   Rename the ``get_superevent`` task to ``get_superevents`` and add
    a new ``get_superevent`` task that is a trivial wrapper around
    ``ligo.gracedb.rest.GraceDb.superevent()``.

0.0.10 (2018-06-13)
-------------------

-   Model the time extent of events and superevents using the
    ``glue.segments`` module.

-   Replace GraceDB.get with GraceDB.superevents from the recent dev
    release of gracedb-client.

-   Fix possible false positive matches between GCNs for unrelated GRBs
    by matching on both TrigID (which is generally the mission elapsed time)
    and mission name.

-   Add the configuration variable ``superevent_far_threshold`` to limit
    the maximum false alarm rate of events that are included in superevents.

-   LVAlert handlers are now passed the actual alert data structure rather than
    the JSON text, so handlers are no longer responsible for calling
    ``json.loads``. It is a little bit more convenient and possibly also faster
    for Celery to deserialize the alert messages.

-   Introduce ``Production``, ``Development``, ``Test``, and ``Playground``
    application configuration objects in order to facilitate quickly switching
    between GraceDB servers.

-   Pipeline specific start and end times for superevent segments. These values
    are controlled via configuration variables.

0.0.9 (2018-06-06)
------------------

-   Add missing LVAlert message types to superevent handler.

0.0.8 (2018-06-06)
------------------

-   Add some logging to the GCN and LVAlert dispatch code in order to
    diagnose missed messages.

0.0.7 (2018-05-31)
------------------

-   Ingest Swift, Fermi, and SNEWS GCN notices and save them in GraceDB.

-   Depend on the pre-release version of the GraceDB client, ligo-gracedb
    1.29.dev0, because this is the only version that supports superevents at
    the moment.

0.0.6 (2018-05-26)
------------------

-   Generate GCN Circular drafts using `ligo-followup-advocate
    <https://git.ligo.org/emfollow/ligo-followup-advocate>`_.

-   In the continuous integration pipeline, validate PEP8 naming conventions
    using `pep8-naming <https://pypi.org/project/pep8-naming/>`_.

-   Add instructions for measuring test coverage and running the linter locally
    to the contributing guide.

-   Rename ``gwcelery.tasks.voevent`` to ``gwcelery.tasks.gcn`` to make it
    clear that this submodule contains functionality related to GCN notices,
    rather than VOEvents in general.

-   Rename ``gwcelery.tasks.dispatch`` to ``gwcelery.tasks.orchestrator`` to
    make it clear that this module encapsulates the behavior associated with
    the "orchestrator" in the O3 low-latency design document.

-   Mock up calls to BAYESTAR in test suite to speed it up.

-   Unify dispatch of LVAlert and GCN messages using decorators.
    GCN notice handlers are declared like this::

        import lxml.etree
        from gwcelery.tasks import gcn

        @gcn.handler(gcn.NoticeType.FERMI_GBM_GND_POS,
                     gcn.NoticeType.FERMI_GBM_FIN_POS)
        def handle_fermi(payload):
            root = lxml.etree.fromstring(payload)
            # do work here...

    LVAlert message handlers are declared like this::

        import json
        from gwcelery.tasks import lvalert

        @lvalert.handler('cbc_gstlal',
                         'cbc_pycbc',
                         'cbc_mbta')
        def handle_cbc(alert_content):
            alert = json.loads(alert_content)
            # do work here...

-   Instead of carrying around the GraceDB service URL in tasks, store the
    GraceDB host name in the Celery application config.

-   Create superevents by simple clustering in time. Currently this is only
    supported by the ``gracedb-dev1`` host.

0.0.5 (2018-05-08)
------------------

-   Disable socket access during most unit tests. This adds some extra
    assurance that we don't accidentally interact with production servers
    during the unit tests.

-   Ignore BAYESTAR jobs that raise a ``DetectorDisabled`` error. These
    exceptions are used for control flow and do not constitute a real error.
    Ignoring these jobs avoids polluting logs and the Flower monitor.

0.0.4 (2018-04-28)
------------------

-   FITS history and comment entries are now displayed in a monospaced font.

-   Adjust error reporting for some tasks.

-   Depend on newer version of ``ligo.skymap``.

-   Add unit tests for the ``gwcelery condor submit`` subcommand.

0.0.3 (2018-04-27)
------------------

-   Fix some compatibility issues between the ``gwcelery condor submit``
    subcommand and the format of ``condor_q -totals -xml`` with older versions
    of HTCondor.

0.0.2 (2018-04-27)
------------------

-   Add ``gwcelery condor submit`` and related subcommands as shortcuts for
    managing GWCelery running under HTCondor.

0.0.1 (2018-04-27)
------------------

-   This is the initial release. It provides rapid sky localization with
    BAYESTAR, sky map annotation, and sending mock alerts.

-   By default, GWCelery is configured to listen to the test LVAlert server.

-   Sending VOEvents to GCN/TAN is disabled for now.
