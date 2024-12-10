"""Tasks that comprise the alert orchestrator.

The orchestrator is responsible for the vetting and annotation workflow to
produce preliminary, initial, and update alerts for gravitational-wave event
candidates.
"""
import json
import re

from astropy.time import Time
from celery import chain, group
from ligo.rrt_chat import channel_creation
from rapidpe_rift_pipe import pastro as rpe_pastro

from .. import app
from . import (alerts, bayestar, circulars, detchar, em_bright,
               external_skymaps, gcn, gracedb, igwn_alert, inference, p_astro,
               rrt_utils, skymaps, superevents)
from .core import get_first, identity


@igwn_alert.handler('superevent',
                    'mdc_superevent',
                    shared=False)
def handle_superevent(alert):
    """Schedule annotations for new superevents.

    After waiting for a time specified by the
    :obj:`~gwcelery.conf.orchestrator_timeout` configuration variable for the
    choice of preferred event to settle down, this task performs data quality
    checks with :meth:`gwcelery.tasks.detchar.check_vectors` and calls
    :meth:`~gwcelery.tasks.orchestrator.earlywarning_preliminary_alert` to send
    a preliminary notice.
    """
    superevent_id = alert['uid']
    # launch PE and detchar based on new type superevents
    if alert['alert_type'] == 'new':
        # launching rapidpe 30s after merger.
        timeout = max(
            alert['object']['t_0'] - Time.now().gps +
            app.conf['rapidpe_timeout'], 0
        )
        (
            gracedb.get_superevent.si(superevent_id).set(
                countdown=timeout
            )
            |
            _get_pe_far_and_event.s()
            |
            parameter_estimation.s(superevent_id, 'rapidpe')
        ).apply_async()

        (
            gracedb.get_superevent.si(superevent_id).set(
                countdown=app.conf['pe_timeout']
            )
            |
            _get_pe_far_and_event.s()
            |
            parameter_estimation.s(superevent_id, 'bilby')
        ).apply_async()

        # run check_vectors. Create and upload omegascans
        group(
            detchar.omegascan.si(alert['object']['t_0'], superevent_id),

            detchar.check_vectors.si(
                alert['object']['preferred_event_data'],
                superevent_id,
                alert['object']['t_start'],
                alert['object']['t_end']
            )
        ).delay()

    elif alert['alert_type'] == 'label_added':
        label_name = alert['data']['name']
        query = f'superevent: {superevent_id} group: CBC Burst'
        if alert['object']['category'] == 'MDC':
            query += ' MDC'
        elif alert['object']['category'] == 'Test':
            query += ' Test'

        # launch less-significant preliminary alerts on LOW_SIGNIF_LOCKED
        if label_name == superevents.FROZEN_LABEL:
            # don't launch if EARLY_WARNING or SIGNIF_LOCKED is present
            skipping_labels = {
                superevents.SIGNIFICANT_LABEL,
                superevents.EARLY_WARNING_LABEL
            }.intersection(alert['object']['labels'])
            if skipping_labels:
                gracedb.upload.delay(
                    None, None, superevent_id,
                    "The superevent already has a significant/EW event, "
                    "skipping launching less-significant alert"
                )
                return
            (
                gracedb.upload.s(
                    None,
                    None,
                    superevent_id,
                    "Automated DQ check before sending less-significant "
                    "preliminary alert. New results supersede old results.",
                    tags=['data_quality']
                )
                |
                detchar.check_vectors.si(
                    alert['object']['preferred_event_data'],
                    superevent_id,
                    alert['object']['t_start'],
                    alert['object']['t_end']
                )
                |
                earlywarning_preliminary_alert.s(
                    alert, alert_type='less-significant')
            ).apply_async()

        # launch significant alert on SIGNIF_LOCKED
        elif label_name == superevents.SIGNIFICANT_LABEL:
            # ensure superevent is locked before starting alert workflow
            r = chain()
            if superevents.FROZEN_LABEL not in alert['object']['labels']:
                r |= gracedb.create_label.si(superevents.FROZEN_LABEL,
                                             superevent_id)

            r |= (
                gracedb.get_events.si(query)
                |
                superevents.select_preferred_event.s()
                |
                _update_superevent_and_return_event_dict.s(superevent_id)
                |
                _leave_log_message_and_return_event_dict.s(
                    superevent_id,
                    "Superevent cleaned up after significant event. "
                )
                |
                _leave_log_message_and_return_event_dict.s(
                    superevent_id,
                    "Automated DQ check before sending significant alert. "
                    "New results supersede old results.",
                    tags=['data_quality']
                )
                |
                detchar.check_vectors.s(
                    superevent_id,
                    alert['object']['t_start'],
                    alert['object']['t_end']
                )
                |
                earlywarning_preliminary_alert.s(
                    alert, alert_type='preliminary')
            )
            r.apply_async()

        # launch second preliminary on GCN_PRELIM_SENT
        elif label_name == 'GCN_PRELIM_SENT':
            (
                identity.si().set(
                    # https://git.ligo.org/emfollow/gwcelery/-/issues/478
                    # FIXME: remove this task once https://github.com/celery/celery/issues/7851 is resolved  # noqa: E501
                    countdown=app.conf['superevent_clean_up_timeout']
                )
                |
                gracedb.get_events.si(query)
                |
                superevents.select_preferred_event.s()
                |
                _update_superevent_and_return_event_dict.s(superevent_id)
                |
                group(
                    _leave_log_message_and_return_event_dict.s(
                        superevent_id,
                        "Superevent cleaned up after first preliminary alert"
                    ),

                    gracedb.create_label.si('DQR_REQUEST', superevent_id)
                )
                |
                get_first.s()
                |
                earlywarning_preliminary_alert.s(
                    alert, alert_type='preliminary')
            ).apply_async()

            # set pipeline preferred events
            # FIXME: Ideally this should combined with the previous canvas.
            # However, incorporating that group prevents canvas from executing
            # maybe related to https://github.com/celery/celery/issues/7851
            (
                gracedb.get_events.si(query)
                |
                superevents.select_pipeline_preferred_event.s()
                |
                _set_pipeline_preferred_events.s(superevent_id)
            ).apply_async(countdown=app.conf['superevent_clean_up_timeout'])

        elif label_name == 'LOW_SIGNIF_PRELIM_SENT':
            # similar workflow as the GCN_PRELIM_SENT
            # except block by condition evaluated at the end of timeout
            _revise_and_send_second_less_significant_alert.si(
                    alert, query, superevent_id,
            ).apply_async(countdown=app.conf['superevent_clean_up_timeout'])

        elif label_name == superevents.EARLY_WARNING_LABEL:
            if superevents.SIGNIFICANT_LABEL in alert['object']['labels']:
                # stop if full BW significant event already present
                gracedb.upload.delay(
                    None, None, superevent_id,
                    "Superevent superseded by full BW event, skipping EW."
                )
                return
            # start the EW alert pipeline; is blocked by SIGNIF_LOCKED
            # ensure superevent is locked before starting pipeline
            r = chain()
            if superevents.FROZEN_LABEL not in alert['object']['labels']:
                r |= gracedb.create_label.si(superevents.FROZEN_LABEL,
                                             superevent_id)

            r |= (
                gracedb.get_events.si(query)
                |
                superevents.select_preferred_event.s()
                |
                _update_superevent_and_return_event_dict.s(superevent_id)
                |
                _leave_log_message_and_return_event_dict.s(
                    superevent_id,
                    "Superevent cleaned up before sending EW alert."
                )
                |
                earlywarning_preliminary_alert.s(
                    alert, alert_type='earlywarning')
            )
            r.apply_async()

        # launch initial/retraction alert on ADVOK/ADVNO
        elif label_name == 'ADVOK':
            initial_alert((None, None, None), alert)
        elif label_name == 'ADVNO':
            retraction_alert(alert)
        elif label_name == 'ADVREQ':
            if app.conf['create_mattermost_channel']:
                _create_mattermost_channel.si(superevent_id).delay()

    # check DQV label on superevent, run check_vectors if required
    elif alert['alert_type'] == 'event_added':
        # FIXME Check if this should be changed to 'update' alert_types instead
        # of 'event_added'. 'event_added' seems like it's just a new event in
        # the superevent window, not necessarily an event that should be
        # promoted to preferred event
        start = alert['data']['t_start']
        end = alert['data']['t_end']

        if 'DQV' in gracedb.get_labels(superevent_id):
            (
                detchar.check_vectors.s(
                    alert['object']['preferred_event_data'],
                    superevent_id,
                    start,
                    end
                )
                |
                _update_if_dqok.s(superevent_id)
            ).apply_async()


@igwn_alert.handler('cbc_gstlal',
                    'cbc_spiir',
                    'cbc_pycbc',
                    'cbc_mbta',
                    shared=False)
def handle_cbc_event(alert):
    """Perform annotations for CBC events that depend on pipeline-specific
    matched-filter parameter estimates.

    Notes
    -----
    This IGWN alert message handler is triggered by a new upload or by updates
    that include the file ``pipeline.p_astro.json``. If also generates
    pipeline.p_astro.json information for pipelines that do not provide
    such information.

    The table below lists which files are created as a result of a new upload,
    and which tasks generate them.

    ============================== ==================================================
    File                           Task
    ============================== ==================================================
    ``bayestar.multiorder.fits``   :meth:`gwcelery.tasks.bayestar.localize`
    ``em_bright.json``             :meth:`gwcelery.tasks.em_bright.source_properties`
    ``pipeline.p_astro.json``      :meth:`gwcelery.tasks.p_astro.compute_p_astro`
    ============================== ==================================================

    """  # noqa: E501
    graceid = alert['uid']
    pipeline = alert['object']['pipeline'].lower()
    search = alert['object']['search'].lower()

    # no annotations for events used in VT analysis
    if search == superevents.VT_SEARCH_NAME.lower():
        return

    priority = 0 if superevents.should_publish(alert['object']) else 1

    # Pipelines that use the GWCelery p-astro method
    # - spiir (all searches)
    # - pycbc for EarlyWarning search
    # - periodic MDC generated by first-two-years (based on gstlal)
    # FIXME: Remove this once all pipelines compute their own p-astro
    pipelines_stock_p_astro = {('spiir', 'earlywarning'),
                               ('pycbc', 'earlywarning'),
                               ('gstlal', 'mdc')}

    # em_bright and p_astro calculation
    if alert['alert_type'] == 'new':
        instruments = superevents.get_instruments_in_ranking_statistic(
            alert['object'])
        extra_attributes = alert['object']['extra_attributes']
        snr = superevents.get_snr(alert['object'])
        far = alert['object']['far']
        mass1 = extra_attributes['SingleInspiral'][0]['mass1']
        mass2 = extra_attributes['SingleInspiral'][0]['mass2']
        chi1 = extra_attributes['SingleInspiral'][0]['spin1z']
        chi2 = extra_attributes['SingleInspiral'][0]['spin2z']

        (
            em_bright.source_properties.si(mass1, mass2, chi1, chi2, snr,
                                           pipeline=pipeline, search=search)
            |
            gracedb.upload.s(
                'em_bright.json', graceid,
                'em bright complete', ['em_bright', 'public']
            )
            |
            gracedb.create_label.si('EMBRIGHT_READY', graceid)
        ).apply_async(priority=priority)

        # p_astro calculation for pipelines that does not provide a
        # stock p_astro (upload pipeline.p_astro.json)
        if (pipeline, search) in pipelines_stock_p_astro:
            (
                p_astro.compute_p_astro.s(snr,
                                          far,
                                          mass1,
                                          mass2,
                                          pipeline,
                                          instruments)
                |
                gracedb.upload.s(
                    f'{pipeline}.p_astro.json', graceid,
                    'p_astro computation complete', ['p_astro', 'public']
                )
                |
                gracedb.create_label.si('PASTRO_READY', graceid)
            ).apply_async(priority=priority)

        # Start BAYESTAR for all CBC pipelines.
        (
            gracedb.download.s('coinc.xml', graceid)
            |
            bayestar.localize.s(graceid)
            |
            gracedb.upload.s(
                'bayestar.multiorder.fits', graceid,
                'sky localization complete', ['sky_loc', 'public']
            )
            |
            gracedb.create_label.si('SKYMAP_READY', graceid)
        ).apply_async(priority=priority)


@igwn_alert.handler('burst_olib',
                    'burst_cwb',
                    'burst_mly',
                    shared=False)
def handle_burst_event(alert):
    """Perform annotations for burst events that depend on pipeline-specific
    """  # noqa: E501
    graceid = alert['uid']
    pipeline = alert['object']['pipeline'].lower()
    search = alert['object']['search'].lower()
    priority = 0 if superevents.should_publish(alert['object']) else 1

    # em_bright calculation for Burst-cWB-BBH
    if alert['alert_type'] == 'new':
        if (pipeline, search) in [('cwb', 'bbh')]:
            extra_attributes = alert['object']['extra_attributes']
            multiburst = extra_attributes['MultiBurst']
            snr = multiburst.get('snr')
            mchirp = multiburst.get('mchirp', 0.0)
            # FIXME once ingestion of mchip is finalised
            # In case mchirp is not there or zero
            # produce em_brigth with all zero
            if mchirp == 0.0:
                m12 = 30.0
            else:
                m12 = 2**(0.2) * mchirp
            (
                em_bright.source_properties.si(m12, m12, 0.0, 0.0, snr)
                |
                gracedb.upload.s(
                    'em_bright.json', graceid,
                    'em bright complete', ['em_bright', 'public']
                )
                |
                gracedb.create_label.si('EMBRIGHT_READY', graceid)
            ).apply_async(priority=priority)

    if alert['alert_type'] != 'log':
        return

    filename = alert['data']['filename']

    # Pipeline is uploading a flat resultion skymap file
    # Converting to a multiorder ones with proper name.
    # FIXME: Remove block when CWB starts to upload skymaps
    # in multiorder format
    if filename.endswith('.fits.gz'):
        new_filename = filename.replace('.fits.gz', '.multiorder.fits')
        flatten_msg = (
            'Multi-resolution FITS file created from '
            '<a href="/api/events/{graceid}/files/'
            '{filename}">{filename}</a>').format(
            graceid=graceid, filename=filename)
        tags = ['sky_loc', 'lvem', 'public']
        (
            gracedb.download.si(filename, graceid)
            |
            skymaps.unflatten.s(new_filename)
            |
            gracedb.upload.s(
                    new_filename, graceid, flatten_msg, tags)
            |
            gracedb.create_label.si('SKYMAP_READY', graceid)
        ).apply_async(priority=priority)


@igwn_alert.handler('superevent',
                    'mdc_superevent',
                    shared=False)
def handle_posterior_samples(alert):
    """Generate multi-resolution and flat-resolution FITS files and skymaps
    from an uploaded HDF5 file containing posterior samples.
    """
    if alert['alert_type'] != 'log' or \
            not alert['data']['filename'].endswith('.posterior_samples.hdf5'):
        return
    superevent_id = alert['uid']
    filename = alert['data']['filename']
    info = '{} {}'.format(alert['data']['comment'], filename)
    prefix, _ = filename.rsplit('.posterior_samples.')
    skymap_filename = f'{prefix}.multiorder.fits'
    labels = ['pe', 'sky_loc']
    ifos = superevents.get_instruments(alert['object']['preferred_event_data'])

    (
        gracedb.download.si(filename, superevent_id)
        |
        skymaps.skymap_from_samples.s(superevent_id, ifos)
        |
        group(
            skymaps.annotate_fits.s(
                skymap_filename, superevent_id, labels
            ),

            gracedb.upload.s(
                skymap_filename, superevent_id,
                'Multiresolution FITS file generated from "{}"'.format(info),
                labels
            )
        )
    ).delay()

    # em_bright from LALInference posterior samples
    (
        gracedb.download.si(filename, superevent_id)
        |
        em_bright.em_bright_posterior_samples.s()
        |
        gracedb.upload.s(
            '{}.em_bright.json'.format(prefix), superevent_id,
            'em-bright computed from "{}"'.format(info),
            'pe'
        )
    ).delay()


@app.task(bind=True, shared=False)
def _create_mattermost_channel(self, superevent_id):
    """
    Creates a mattermost channel when ADVREQ label is applied and
    posts a cooresponding gracedb link of that event in the channel

    Channel name : O4 RRT {superevent_id}

    Parameters:
    ------------
    superevent_id: str
        The superevent id
    """
    gracedb_url = self.app.conf['gracedb_host']
    channel_creation.rrt_channel_creation(
        superevent_id, gracedb_url)


@app.task(shared=False)
def _set_pipeline_preferred_events(pipeline_event, superevent_id):
    """Return group for setting pipeline preferred event using
    :meth:`gracedb.add_pipeline_preferred_event`.

    Parameters
    ----------
    pipeline_event: dict
        {pipeline: event_dict} key value pairs, returned by
        :meth:`superevents.select_pipeline_preferred_event`.

    superevent_id: str
        The superevent id
    """
    return group(
        gracedb.add_pipeline_preferred_event(superevent_id,
                                             event['graceid'])
        for event in pipeline_event.values()
    )


@app.task(shared=False, ignore_result=True)
def _update_if_dqok(event, superevent_id):
    """Update `preferred_event` of `superevent_id` to `event_id` if `DQOK`
    label has been applied.
    """
    if 'DQOK' in gracedb.get_labels(superevent_id):
        event_id = event['graceid']
        gracedb.update_superevent(superevent_id,
                                  preferred_event=event_id,
                                  t_0=event["gpstime"])
        gracedb.upload.delay(
            None, None, superevent_id,
            comment=f'DQOK applied based on new event {event_id}')


@gracedb.task(shared=False)
def _create_voevent(classification, *args, **kwargs):
    r"""Create a VOEvent record from an EM bright JSON file.

    Parameters
    ----------
    classification : tuple, None
        A collection of JSON strings, generated by
        :meth:`gwcelery.tasks.em_bright.source_properties` and
        :meth:`gwcelery.tasks.p_astro.compute_p_astro` or
        content of ``{gstlal,mbta}.p_astro.json`` uploaded
        by {gstlal,mbta} respectively; or None
    \*args
        Additional positional arguments passed to
        :meth:`gwcelery.tasks.gracedb.create_voevent`.
    \*\*kwargs
        Additional keyword arguments passed to
        :meth:`gwcelery.tasks.gracedb.create_voevent`.

    Returns
    -------
    str
        The filename of the newly created VOEvent.

    """
    kwargs = dict(kwargs)

    if classification is not None:
        # Merge source classification and source properties into kwargs.
        for text in classification:
            # Ignore filenames, only load dict in bytes form
            if text is not None:
                kwargs.update(json.loads(text))

    # FIXME: These keys have differ between em_bright.json
    # and the GraceDB REST API.
    try:
        kwargs['ProbHasNS'] = kwargs.pop('HasNS')
    except KeyError:
        pass

    try:
        kwargs['ProbHasRemnant'] = kwargs.pop('HasRemnant')
    except KeyError:
        pass

    skymap_filename = kwargs.get('skymap_filename')
    if skymap_filename is not None:
        skymap_type = re.sub(
            r'(\.multiorder)?\.fits(\..+)?(,[0-9]+)?$', '', skymap_filename)
        kwargs.setdefault('skymap_type', skymap_type)

    # FIXME: remove ._orig_run when this bug is fixed:
    # https://github.com/getsentry/sentry-python/issues/370
    return gracedb.create_voevent._orig_run(*args, **kwargs)


@app.task(shared=False)
def _create_label_and_return_filename(filename, label, graceid):
    gracedb.create_label.delay(label, graceid)
    return filename


@app.task(shared=False)
def _leave_log_message_and_return_event_dict(event, superevent_id,
                                             message, **kwargs):
    """Wrapper around :meth:`gracedb.upload`
    that returns the event dictionary.
    """
    gracedb.upload.delay(None, None, superevent_id, message, **kwargs)
    return event


@gracedb.task(shared=False)
def _update_superevent_and_return_event_dict(event, superevent_id):
    """Wrapper around :meth:`gracedb.update_superevent`
    that returns the event dictionary.
    """
    gracedb.update_superevent(superevent_id,
                              preferred_event=event['graceid'],
                              t_0=event['gpstime'])
    return event


@gracedb.task(shared=False)
def _proceed_if_not_blocked_by(files, superevent_id, block_by):
    """Return files in case the superevent does not have labels `block_by`

    Parameters
    ----------
    files : tuple
        List of files
    superevent_id : str
        The superevent id corresponding to files
    block_by : set
        Set of blocking labels. E.g. `{'ADVOK', 'ADVNO'}`
    """
    superevent_labels = gracedb.get_labels(superevent_id)
    blocking_labels = block_by.intersection(superevent_labels)
    if blocking_labels:
        gracedb.upload.delay(
            None, None, superevent_id,
            f"Blocking automated notice due to labels {blocking_labels}"
        )
        return None
    else:
        return files


@gracedb.task(shared=False)
def _revise_and_send_second_less_significant_alert(alert, query,
                                                   superevent_id):
    superevent_labels = gracedb.get_labels(superevent_id)
    blocking_labels = {
        'ADVREQ', 'ADVOK', 'ADVNO',
        superevents.SIGNIFICANT_LABEL,
        superevents.EARLY_WARNING_LABEL,
    }
    if blocking_labels.intersection(superevent_labels):
        return

    (
        gracedb.get_events.si(query)
        |
        superevents.select_preferred_event.s()
        |
        _update_superevent_and_return_event_dict.s(superevent_id)
        |
        _leave_log_message_and_return_event_dict.s(
            superevent_id,
            "Superevent cleaned up before second less-significant alert"
        )
        |
        earlywarning_preliminary_alert.s(
            alert, alert_type='less-significant')
    ).delay()

    # set pipeline preferred events
    # FIXME: Ideally this should combined with the previous canvas.
    # However, incorporating that group prevents canvas from executing
    # maybe related to https://github.com/celery/celery/issues/7851
    (
        gracedb.get_events.si(query)
        |
        superevents.select_pipeline_preferred_event.s()
        |
        _set_pipeline_preferred_events.s(superevent_id)
    ).delay()


@app.task(shared=False)
def _annotate_fits_and_return_input(input_list, superevent_id):
    """Unpack the output of the skymap, embright, p-astro download group in the
    beginning of the
    :meth:`~gwcelery.tasks.orchestartor.earlywarning_preliminary_alert` canvas
    and call :meth:`~gwcelery.tasks.skymaps.annotate_fits`.


    Parameters
    ----------
    input_list : list
        The output of the group that downloads the skymap, embright, and
        p-astro files. This list is in the form [skymap, skymap_filename],
        [em_bright, em_bright_filename], [p_astro_dict, p_astro_filename],
        though the em-bright and p-astro lists can be populated by Nones
    superevent_id : str
        A list of the sky map, em_bright, and p_astro filenames.
    """

    skymaps.annotate_fits_tuple(
        input_list[0],
        superevent_id,
        ['sky_loc', 'public']
    )

    return input_list


@gracedb.task(shared=False)
def _unpack_args_and_send_earlywarning_preliminary_alert(input_list, alert,
                                                         alert_type):
    """Unpack the output of the skymap, embright, p-astro download group in the
    beginning of the
    :meth:`~gwcelery.tasks.orchestartor.earlywarning_preliminary_alert` canvas
    and call
    :meth:`gwcelery.tasks.orchestrator.earlywarning_preliminary_initial_update_alert`.


    Parameters
    ----------
    input_list : list
        The output of the group that downloads the skymap, embright, and
        p-astro files. This list is in the form [skymap, skymap_filename],
        [em_bright, em_bright_filename], [p_astro_dict, p_astro_filename],
        though the em-bright and p-astro lists can be populated by Nones
    alert : dict
        IGWN-Alert dictionary
    alert_type : str
        alert_type passed to
        :meth:`earlywarning_preliminary_initial_update_alert`
    """
    if input_list is None:  # alert is blocked by blocking labels
        return

    [skymap, skymap_filename], [em_bright, em_bright_filename], \
        [p_astro_dict, p_astro_filename] = input_list

    # Update to latest state after downloading files
    superevent = gracedb.get_superevent(alert['object']['superevent_id'])

    earlywarning_preliminary_initial_update_alert.delay(
        [skymap_filename, em_bright_filename, p_astro_filename],
        superevent, alert_type,
        filecontents=[skymap, em_bright, p_astro_dict]
    )


@app.task(ignore_result=True, shared=False)
def earlywarning_preliminary_alert(event, alert, alert_type='preliminary',
                                   initiate_voevent=True):
    """Produce a preliminary alert by copying any sky maps.

    This consists of the following steps:

    1.   Copy any sky maps and source classification from the preferred event
         to the superevent.
    2.   Create standard annotations for sky maps including all-sky plots by
         calling :meth:`gwcelery.tasks.skymaps.annotate_fits`.
    3.   Create a preliminary VOEvent.
    4.   Send the VOEvent to GCN and notices to SCiMMA and GCN.
    5.   Apply the GCN_PRELIM_SENT or LOW_SIGNIF_PRELIM_SENT
         depending on the significant or less-significant alert
         respectively.
    6.   Create and upload a GCN Circular draft.
    """
    priority = 0 if superevents.should_publish(event) else 1
    preferred_event_id = event['graceid']
    superevent_id = alert['uid']

    # Define alert payloads depending on group-pipeline-search
    # cbc-*-*         :  p_astro/em_bright
    # burst.cwb-bbh  :  p_astro/em_bright
    # burst-*-*       :  NO p_astro/em_bright
    alert_group = event['group'].lower()
    alert_pipeline = event['pipeline'].lower()
    alert_search = event['search'].lower()
    if alert_pipeline == 'cwb' and alert_search == 'bbh':
        skymap_filename = alert_pipeline + '.multiorder.fits'
        p_astro_filename = alert_pipeline + '.p_astro.json'
        em_bright_filename = 'em_bright.json'
    elif alert_group == 'cbc':
        skymap_filename = 'bayestar.multiorder.fits'
        p_astro_filename = alert_pipeline + '.p_astro.json' \
            if alert_search != 'ssm' else None
        em_bright_filename = 'em_bright.json'
    elif alert_group == 'burst':
        skymap_filename = event['pipeline'].lower() + '.multiorder.fits'
        p_astro_filename = None
        em_bright_filename = None
    else:
        raise NotImplementedError(
            'Valid skymap required for preliminary alert'
        )

    # Determine if the event should be made public.
    is_publishable = (superevents.should_publish(
                      event, significant=alert_type != 'less-significant') and
                      {'DQV', 'INJ'}.isdisjoint(
                      event['labels']))

    # Download files from events and upload to superevent with relevant
    # annotations. Pass file contents down the chain so alerts task doesn't
    # need to download them again.
    # Note: this is explicitly made a chain to fix an issue described in #464.
    canvas = chain(
        group(
            gracedb.download.si(skymap_filename, preferred_event_id)
            |
            group(
                identity.s(),

                gracedb.upload.s(
                    skymap_filename,
                    superevent_id,
                    message='Localization copied from {}'.format(
                        preferred_event_id),
                    tags=['sky_loc', 'public']
                )
                |
                _create_label_and_return_filename.s('SKYMAP_READY',
                                                    superevent_id)
            ),

            (
                gracedb.download.si(em_bright_filename, preferred_event_id)
                |
                group(
                    identity.s(),

                    gracedb.upload.s(
                        em_bright_filename,
                        superevent_id,
                        message='Source properties copied from {}'.format(
                            preferred_event_id),
                        tags=['em_bright', 'public']
                    )
                    |
                    _create_label_and_return_filename.s('EMBRIGHT_READY',
                                                        superevent_id)
                )
            ) if em_bright_filename is not None else
            identity.s([None, None]),

            (
                gracedb.download.si(p_astro_filename, preferred_event_id)
                |
                group(
                    identity.s(),

                    gracedb.upload.s(
                        p_astro_filename,
                        superevent_id,
                        message='Source classification copied from {}'.format(
                            preferred_event_id),
                        tags=['p_astro', 'public']
                    )
                    |
                    _create_label_and_return_filename.s('PASTRO_READY',
                                                        superevent_id)
                )
            ) if p_astro_filename is not None else
            identity.s([None, None])
        )
        |
        # Need annotate skymap task in body of chord instead of header because
        # this task simply calls another task, which is to be avoided in chord
        # headers. Note that any group that chains to a task is automatically
        # upgraded to a chord.
        _annotate_fits_and_return_input.s(superevent_id)
    )

    # Switch for disabling all but MDC alerts.
    if app.conf['only_alert_for_mdc']:
        if event.get('search') != 'MDC':
            canvas |= gracedb.upload.si(
                None, None, superevent_id,
                ("Skipping alert because gwcelery has been configured to only"
                 " send alerts for MDC events."))
            canvas.apply_async(priority=priority)
            return

    # Send notice and upload GCN circular draft for online events.
    if is_publishable and initiate_voevent:
        # presence of advocate action blocks significant prelim alert
        # presence of adv action or significant event blocks EW alert
        # presence of adv action or significant event or EW event blocks
        # less significant alert
        if alert_type == 'earlywarning':
            # Launch DQR for significant early warning events after a timeout.
            # If a full BW significant trigger arrives before the end of the
            # timeout, the latter will apply the label instead, and this call
            # is a noop.
            gracedb.create_label.si(
                'DQR_REQUEST',
                superevent_id
            ).apply_async(countdown=600)
        blocking_labels = (
            {'ADVOK', 'ADVNO'} if alert_type == 'preliminary'
            else
            {superevents.SIGNIFICANT_LABEL, 'ADVOK', 'ADVNO'}
            if alert_type == 'earlywarning'
            else
            {superevents.EARLY_WARNING_LABEL, superevents.SIGNIFICANT_LABEL,
             'ADVOK', 'ADVNO'}
            if alert_type == 'less-significant'
            else
            set()
        )
        canvas |= (
            _proceed_if_not_blocked_by.s(superevent_id, blocking_labels)
            |
            _unpack_args_and_send_earlywarning_preliminary_alert.s(
                alert, alert_type
            )
        )

    canvas.apply_async(priority=priority)


@gracedb.task(shared=False)
def _get_pe_far_and_event(superevent):
    """Return FAR and event input to PE workflow.

    The input FAR is the lowest FAR among CBC and Burst-BBH triggers.
    The input event is the preferred event if it is a CBC trigger, otherwise
    the CBC trigger with the lowest FAR is returned.
    """
    # FIXME: remove ._orig_run when this bug is fixed:
    # https://github.com/getsentry/sentry-python/issues/370
    events = [
        gracedb.get_event._orig_run(gid) for gid in superevent['gw_events']
    ]
    events = [
        e for e in events if e['group'].lower() == 'cbc' or (
            e['group'].lower() == 'burst' and
            e.get('search', 'None').lower() == 'bbh'
        )
    ]
    events.sort(key=lambda e: e['far'])
    preferred_event = superevent['preferred_event_data']

    if preferred_event['group'].lower() == 'cbc':
        return events[0]['far'], preferred_event
    for e in events:
        if e['group'].lower() == 'cbc':
            return events[0]['far'], e
    return None


@app.task(ignore_result=True, shared=False)
def parameter_estimation(far_event, superevent_id, pe_pipeline):
    """Parameter Estimation with Bilby and RapidPE-RIFT. Parameter estimation
    runs are triggered for CBC triggers which pass the FAR threshold and are
    not mock uploads. For those which do not pass these criteria, this task
    uploads messages explaining why parameter estimation is not started.
    """
    if far_event is None:
        gracedb.upload.delay(
            filecontents=None, filename=None,
            graceid=superevent_id,
            message='Parameter estimation will not start since no CBC triggers'
                    ' are found.',
            tags='pe'
        )
        return
    far, event = far_event
    search = event['search'].lower()
    if search in app.conf['significant_alert_far_threshold']['cbc']:
        threshold = (
            app.conf['significant_alert_far_threshold']['cbc'][search] /
            app.conf['significant_alert_trials_factor']['cbc'][search]
        )
    else:
        # Fallback in case an event is uploaded to an unlisted search
        threshold = -1 * float('inf')
    if far > threshold:
        gracedb.upload.delay(
            filecontents=None, filename=None,
            graceid=superevent_id,
            message='Parameter estimation will not start since FAR is larger '
                    'than the PE threshold, {}  Hz.'.format(threshold),
            tags='pe'
        )
    elif search == 'mdc':
        gracedb.upload.delay(
            filecontents=None, filename=None,
            graceid=superevent_id,
            message='Parameter estimation will not start since parameter '
                    'estimation is disabled for mock uploads.',
            tags='pe'
        )
    elif event.get('offline', False):
        gracedb.upload.delay(
            filecontents=None, filename=None,
            graceid=superevent_id,
            message='Parameter estimation will not start since parameter '
                    'estimation is disabled for OFFLINE events.',
            tags='pe'
        )
    elif (
        app.conf['gracedb_host'] == 'gracedb-playground.ligo.org'
        and event['pipeline'] == 'MBTA'
    ):
        # FIXME: Remove this block once multiple channels can be handled on
        # one gracedb instance
        gracedb.upload.delay(
            filecontents=None, filename=None,
            graceid=superevent_id,
            message='Parameter estimation is disabled for MBTA triggers '
                    'on playground as MBTA analyses live data + online '
                    'injections not O3 replay data + MDC injections',
            tags='pe'
        )
    elif pe_pipeline == 'rapidpe' and search == 'earlywarning':
        # Remove this if rapidpe can ingest early warning events
        gracedb.upload.delay(
            filecontents=None, filename=None,
            graceid=superevent_id,
            message='Parameter estimation by RapidPE-RIFT is disabled for '
                    'earlywarning triggers.',
            tags='pe'
        )
    else:
        inference.start_pe.delay(event, superevent_id, pe_pipeline)


@gracedb.task(shared=False)
def earlywarning_preliminary_initial_update_alert(
    filenames,
    superevent,
    alert_type,
    filecontents=None
):
    """
    Create a canvas that sends an earlywarning, preliminary, initial, or update
    notice.

    Parameters
    ----------
    filenames : tuple
        A list of the sky map, em_bright, and p_astro filenames.
    superevent : dict
        The superevent dictionary, typically obtained from an IGWN Alert or
        from querying GraceDB.
    alert_type : {'less-significant', 'earlywarning', 'preliminary', 'initial', 'update'}  # noqa: E501
        The alert type.

    Notes
    -----
    Tasks that call this function should be decorated with
    :obj:`gwcelery.tasks.gracedb.task` rather than :obj:`gwcelery.app.task` so
    that a synchronous call to :func:`gwcelery.tasks.gracedb.get_log` is
    retried in the event of GraceDB API failures. If `EM_COINC` is in labels
    will create a RAVEN circular.

    """
    labels = superevent['labels']
    superevent_id = superevent['superevent_id']
    search = superevent['preferred_event_data']['search'].lower()

    if 'INJ' in labels:
        return

    if filecontents:
        assert alert_type in {
            'less-significant', 'earlywarning', 'preliminary'}

    skymap_filename, em_bright_filename, p_astro_filename = filenames
    rapidpe_pastro_filename = None
    rapidpe_pastro_needed = True
    combined_skymap_filename = None
    combined_skymap_needed = False
    skymap_needed = (skymap_filename is None)
    em_bright_needed = (em_bright_filename is None)
    p_astro_needed = False if search == 'ssm' else (p_astro_filename is None)
    raven_coinc = ('RAVEN_ALERT' in labels and bool(superevent['em_type']))
    if raven_coinc:
        ext_labels = gracedb.get_labels(superevent['em_type'])
        combined_skymap_needed = \
            {"RAVEN_ALERT", "COMBINEDSKYMAP_READY"}.issubset(set(ext_labels))

    # FIXME: This if statement is always True, we should get rid of it.
    if skymap_needed or em_bright_needed or p_astro_needed or \
            combined_skymap_needed or rapidpe_pastro_needed:
        for message in gracedb.get_log(superevent_id):
            t = message['tag_names']
            f = message['filename']
            v = message['file_version']
            fv = '{},{}'.format(f, v)
            if not f:
                continue
            if skymap_needed \
                    and {'sky_loc', 'public'}.issubset(t) \
                    and f.endswith('.multiorder.fits') \
                    and 'combined' not in f:
                skymap_filename = fv
            if em_bright_needed \
                    and 'em_bright' in t \
                    and f.endswith('.json'):
                em_bright_filename = fv
            if p_astro_needed \
                    and {'p_astro', 'public'}.issubset(t) \
                    and f.endswith('.json'):
                p_astro_filename = fv
            if combined_skymap_needed \
                    and {'sky_loc', 'ext_coinc'}.issubset(t) \
                    and f.startswith('combined-ext.') \
                    and 'fit' in f:
                combined_skymap_filename = fv
            if rapidpe_pastro_needed \
                    and {'p_astro', 'public'}.issubset(t) \
                    and f == 'RapidPE_RIFT.p_astro.json':
                rapidpe_pastro_filename = fv

    if combined_skymap_needed:
        # for every alert, copy combined sky map over if applicable
        # FIXME: use file inheritance once available
        ext_id = superevent['em_type']
        if combined_skymap_filename:
            # If previous sky map, increase version by 1
            combined_skymap_filename_base, v = \
                combined_skymap_filename.split(',')
            v = str(int(v) + 1)
            combined_skymap_filename = \
                combined_skymap_filename_base + ',' + v
        else:
            combined_skymap_filename_base = \
                (external_skymaps.COMBINED_SKYMAP_FILENAME_MULTIORDER
                 if '.multiorder.fits' in skymap_filename else
                 external_skymaps.COMBINED_SKYMAP_FILENAME_FLAT)
            combined_skymap_filename = combined_skymap_filename_base + ',0'
        message = 'Combined LVK-external sky map copied from {0}'.format(
            ext_id)
        message_png = (
            'Mollweide projection of <a href="/api/superevents/{se_id}/files/'
            '{filename}">{filename}</a>, copied from {ext_id}').format(
               se_id=superevent_id,
               ext_id=ext_id,
               filename=combined_skymap_filename)

        combined_skymap_canvas = group(
            gracedb.download.si(combined_skymap_filename_base, ext_id)
            |
            gracedb.upload.s(
                combined_skymap_filename_base, superevent_id,
                message, ['sky_loc', 'ext_coinc', 'public'])
            |
            gracedb.create_label.si('COMBINEDSKYMAP_READY', superevent_id),

            gracedb.download.si(external_skymaps.COMBINED_SKYMAP_FILENAME_PNG,
                                ext_id)
            |
            gracedb.upload.s(
                external_skymaps.COMBINED_SKYMAP_FILENAME_PNG, superevent_id,
                message_png, ['sky_loc', 'ext_coinc', 'public']
            )
            |
            # Pass None to download_anor_expose group
            identity.si()
        )

    # circular template not needed for less-significant alerts
    if alert_type in {'earlywarning', 'preliminary', 'initial'}:
        if raven_coinc:
            circular_task = circulars.create_emcoinc_circular.si(superevent_id)
            circular_filename = '{}-emcoinc-circular.txt'.format(alert_type)
            tags = ['em_follow', 'ext_coinc']

        else:
            circular_task = circulars.create_initial_circular.si(superevent_id)
            circular_filename = '{}-circular.txt'.format(alert_type)
            tags = ['em_follow']

        circular_canvas = (
            circular_task
            |
            gracedb.upload.s(
                circular_filename,
                superevent_id,
                'Template for {} GCN Circular'.format(alert_type),
                tags=tags)
        )

    else:
        circular_canvas = identity.si()

    # less-significant alerts have "preliminary" voevent notice type
    alert_type_voevent = 'preliminary' if alert_type == 'less-significant' \
        else alert_type
    # set the significant field in the VOEvent based on
    # less-significant/significant alert.
    # For kafka alerts the analogous field is set in alerts.py.
    # (see comment before defining kafka_alert_canvas)
    voevent_significance = 0 if alert_type == 'less-significant' else 1

    if filecontents and not combined_skymap_filename:
        skymap, em_bright, p_astro_dict = filecontents

        # check high profile and apply label if true
        if alert_type == 'preliminary':
            high_profile_canvas = rrt_utils.check_high_profile.si(
                skymap, em_bright, p_astro_dict, superevent
            )
        else:
            high_profile_canvas = identity.si()

        download_andor_expose_group = []
        if rapidpe_pastro_filename is None:
            voevent_canvas = _create_voevent.si(
                (em_bright, p_astro_dict),
                superevent_id,
                alert_type_voevent,
                Significant=voevent_significance,
                skymap_filename=skymap_filename,
                internal=False,
                open_alert=True,
                raven_coinc=raven_coinc,
                combined_skymap_filename=combined_skymap_filename
            )
            rapidpe_canvas = _update_rapidpe_pastro_shouldnt_run.s()

            # kafka alerts have a field called "significant" based on
            # https://dcc.ligo.org/LIGO-G2300151/public
            # The alert_type value passed to alerts.send is used to
            # set this field in the alert dictionary
            kafka_alert_canvas = alerts.send.si(
                (skymap, em_bright, p_astro_dict),
                superevent,
                alert_type,
                raven_coinc=raven_coinc
            )
        else:
            voevent_canvas = _create_voevent.s(
                superevent_id,
                alert_type_voevent,
                Significant=voevent_significance,
                skymap_filename=skymap_filename,
                internal=False,
                open_alert=True,
                raven_coinc=raven_coinc,
                combined_skymap_filename=combined_skymap_filename
            )
            download_andor_expose_group += [
                gracedb.download.si(rapidpe_pastro_filename, superevent_id)
            ]

            kafka_alert_canvas = _check_pastro_and_send_alert.s(
                skymap,
                em_bright,
                superevent,
                alert_type,
                raven_coinc=raven_coinc
            )

            rapidpe_canvas = (
                _update_rapidpe_pastro.s(
                    em_bright=em_bright,
                    pipeline_pastro=p_astro_dict)
                |
                _upload_rapidpe_pastro_json.s(
                    superevent_id,
                    rapidpe_pastro_filename
                )
            )
    else:
        # Download em_bright and p_astro files here for voevent
        download_andor_expose_group = [
            gracedb.download.si(em_bright_filename, superevent_id) if
            em_bright_filename is not None else identity.s(None),
        ]
        if search != 'ssm':
            download_andor_expose_group += [
                gracedb.download.si(p_astro_filename, superevent_id) if
                p_astro_filename is not None else identity.s(None)
            ]
        else:
            # for SSM events skip downloading p-astro file
            download_andor_expose_group += [identity.s(None)]
        high_profile_canvas = identity.si()

        voevent_canvas = _create_voevent.s(
            superevent_id,
            alert_type_voevent,
            Significant=voevent_significance,
            skymap_filename=skymap_filename,
            internal=False,
            open_alert=True,
            raven_coinc=raven_coinc,
            combined_skymap_filename=combined_skymap_filename
        )

        if rapidpe_pastro_filename:
            download_andor_expose_group += [
                    gracedb.download.si(rapidpe_pastro_filename, superevent_id)
            ]

            rapidpe_canvas = (
                _update_rapidpe_pastro.s()
                |
                _upload_rapidpe_pastro_json.s(
                    superevent_id,
                    rapidpe_pastro_filename
                )
            )

        # The skymap has not been downloaded at this point, so we need to
        # download it before we can assemble the kafka alerts and send them
        kafka_alert_canvas = alerts.download_skymap_and_send_alert.s(
            superevent,
            alert_type,
            skymap_filename=skymap_filename,
            raven_coinc=raven_coinc,
            combined_skymap_filename=combined_skymap_filename
        )

    to_expose = [skymap_filename, em_bright_filename, p_astro_filename]
    # Since PE skymap images, HTML, and gzip FITS are not made public when they
    # are uploaded, we need to expose them here.
    if (
        skymap_filename is not None and 'bilby' in skymap_filename.lower()
    ):
        prefix, _, _ = skymap_filename.partition('.multiorder.fits')
        to_expose += [f'{prefix}.html', f'{prefix}.png',
                      f'{prefix}.volume.png', f'{prefix}.fits.gz']
    download_andor_expose_group += [
        gracedb.expose.si(superevent_id),
        *(
            gracedb.create_tag.si(filename, 'public', superevent_id)
            for filename in to_expose if filename is not None
        )
    ]

    voevent_canvas |= group(
        gracedb.download.s(superevent_id)
        |
        gcn.send.s(),

        gracedb.create_tag.s('public', superevent_id)
    )

    if combined_skymap_needed:
        download_andor_expose_group += [combined_skymap_canvas]

    sent_label_canvas = identity.si()
    if alert_type == 'less-significant':
        sent_label_canvas = gracedb.create_label.si(
            'LOW_SIGNIF_PRELIM_SENT',
            superevent_id
        )
    elif alert_type == 'preliminary':
        sent_label_canvas = gracedb.create_label.si(
            'GCN_PRELIM_SENT',
            superevent_id
        )

    # NOTE: The following canvas structure was used to fix #480
    canvas = (
        group(download_andor_expose_group)
    )
    if rapidpe_pastro_filename:
        canvas |= rapidpe_canvas

    canvas |= (
        group(
            voevent_canvas
            |
            group(
                circular_canvas,

                sent_label_canvas
            ),

            kafka_alert_canvas,

            high_profile_canvas
        )
    )

    canvas.apply_async()


@app.task(shared=False)
def _update_rapidpe_pastro(input_list, em_bright=None, pipeline_pastro=None):
    """
    If p_terr from rapidpe is different from the p_terr from the most
    recent preferred event, replaces rapidpe's p_terr with pipeline p_terr.
    Returns a tuple of em_bright, rapidpe pastro and a
    boolean(rapidpe_pastro_updated) indicating if rapidpe pastro has been
    updated. If p_terr in rapidpe has been updated, the return list contains
    the updated pastro and the rapidpe_pastro_updated is True. Else, the
    return list contains the rapidpe pastro from the input_list and
    rapidpe_pastro_updated is False.
    """
    # input_list is download_andor_expose_group in
    # function earlywarning_preliminary_initial_update_alert
    if pipeline_pastro is None:
        em_bright, pipeline_pastro, rapidpe_pastro, *_ = input_list
    else:
        rapidpe_pastro, *_ = input_list
    pipeline_pastro_contents = json.loads(pipeline_pastro)
    rapidpe_pastro_contents = json.loads(rapidpe_pastro)

    if (rapidpe_pastro_contents["Terrestrial"]
            == pipeline_pastro_contents["Terrestrial"]):
        rapidpe_pastro_updated = False
    else:
        rapidpe_pastro = json.dumps(
            rpe_pastro.renormalize_pastro_with_pipeline_pterr(
                rapidpe_pastro_contents, pipeline_pastro_contents
            )
        )
        rapidpe_pastro_updated = True

    return em_bright, rapidpe_pastro, rapidpe_pastro_updated


@app.task(shared=False)
def _update_rapidpe_pastro_shouldnt_run():
    raise RuntimeError(
        "The `rapidpe_canvas' was executed where it should"
        "not have been.  A bug must have been introduced."
    )


@gracedb.task(shared=False)
def _upload_rapidpe_pastro_json(
    input_list,
    superevent_id,
    rapidpe_pastro_filename
):
    """
    Add public tag to RapidPE_RIFT.p_astro.json if p_terr from the
    preferred event  is same as the p_terr in RapidPE_RIFT.p_astro.json.
    Else, uploads an updated version of RapidPE_RIFT.p_astro.json
    with file content from  the task update_rapidpe_pastro.
    """
    # input_list is output from update_rapidpe_pastro
    *return_list, rapidpe_pastro_updated = input_list
    if rapidpe_pastro_updated is True:
        tags = ("pe", "p_astro", "public")

        upload_filename = "RapidPE_RIFT.p_astro.json"
        description = "RapidPE-RIFT Pastro results"
        content = input_list[1]
        gracedb.upload(
            content,
            upload_filename,
            superevent_id,
            description,
            tags
        )
    return return_list


@app.task(shared=False)
def _check_pastro_and_send_alert(input_classification, skymap, em_bright,
                                 superevent, alert_type, raven_coinc=False):
    """Wrapper for :meth:`~gwcelery.tasks.alerts.send` meant to take a
    potentially new p-astro as input from the preceding task.
    """
    _, p_astro = input_classification
    alerts.send.delay(
        (skymap, em_bright, p_astro),
        superevent,
        alert_type,
        raven_coinc=raven_coinc
    )


@gracedb.task(ignore_result=True, shared=False)
def initial_alert(filenames, alert):
    """Produce an initial alert.

    This does nothing more than call
    :meth:`~gwcelery.tasks.orchestrator.earlywarning_preliminary_initial_update_alert`
    with ``alert_type='initial'``.

    Parameters
    ----------
    filenames : tuple
        A list of the sky map, em_bright, and p_astro filenames.
    alert : dict
        IGWN-Alert dictionary

    Notes
    -----
    This function is decorated with :obj:`gwcelery.tasks.gracedb.task` rather
    than :obj:`gwcelery.app.task` so that a synchronous call to
    :func:`gwcelery.tasks.gracedb.get_log` is retried in the event of GraceDB
    API failures.

    """
    earlywarning_preliminary_initial_update_alert(
        filenames,
        alert['object'],
        'initial'
    )


@gracedb.task(ignore_result=True, shared=False)
def update_alert(filenames, superevent_id):
    """Produce an update alert.

    This does nothing more than call
    :meth:`~gwcelery.tasks.orchestrator.earlywarning_preliminary_initial_update_alert`
    with ``alert_type='update'``.

    Parameters
    ----------
    filenames : tuple
        A list of the sky map, em_bright, and p_astro filenames.
    superevent_id : str
        The superevent ID.

    Notes
    -----
    This function is decorated with :obj:`gwcelery.tasks.gracedb.task` rather
    than :obj:`gwcelery.app.task` so that a synchronous call to
    :func:`gwcelery.tasks.gracedb.get_log` is retried in the event of GraceDB
    API failures.

    """
    superevent = gracedb.get_superevent._orig_run(superevent_id)
    earlywarning_preliminary_initial_update_alert(
        filenames,
        superevent,
        'update'
    )


@app.task(ignore_result=True, shared=False)
def retraction_alert(alert):
    """Produce a retraction alert."""
    superevent_id = alert['uid']
    group(
        gracedb.expose.si(superevent_id)
        |
        group(
            _create_voevent.si(
                None, superevent_id, 'retraction',
                internal=False,
                open_alert=True
            )
            |
            group(
                gracedb.download.s(superevent_id)
                |
                gcn.send.s(),

                gracedb.create_tag.s('public', superevent_id)
            ),

            alerts.send.si(
                None,
                alert['object'],
                'retraction'
            )
        ),

        circulars.create_retraction_circular.si(superevent_id)
        |
        gracedb.upload.s(
            'retraction-circular.txt',
            superevent_id,
            'Template for retraction GCN Circular',
            tags=['em_follow']
        )
    ).apply_async()
