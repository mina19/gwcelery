"""Flask web application views."""
import datetime
import json
import os
import platform
import re
import socket
import sys
from importlib import metadata

import lal
from astropy.time import Time
from celery import group
from flask import (flash, jsonify, make_response, redirect, render_template,
                   request, url_for)
from requests.exceptions import HTTPError

from . import app as celery_app
from ._version import get_versions
from .flask import app, cache
from .tasks import (bayestar, circulars, core, external_skymaps, first2years,
                    first2years_external, gracedb, orchestrator, skymaps,
                    superevents)
from .util import PromiseProxy

# Change the application root url
PREFIX = os.getenv('FLASK_APP_PREFIX', '')

distributions = PromiseProxy(lambda: tuple(metadata.distributions()))


@app.route(PREFIX + '/')
def index():
    """Render main page."""
    return render_template(
        'index.jinja2',
        conf=celery_app.conf,
        hostname=socket.getfqdn(),
        detectors=sorted(lal.cached_detector_by_prefix.keys()),
        distributions=distributions,
        platform=platform.platform(),
        versions=get_versions(),
        python_version=sys.version,
        joint_mdc_freq=celery_app.conf['joint_mdc_freq'])


def take_n(n, iterable):
    """Take the first `n` items of a collection."""
    for i, item in enumerate(iterable):
        if i >= n:
            break
        yield item


# Regular expression for parsing query strings
# that look like GraceDB superevent names.
_typeahead_superevent_id_regex = re.compile(
    r'(?P<prefix>[MT]?)S?(?P<date>\d{0,6})(?P<suffix>[a-z]*)',
    re.IGNORECASE)


@app.route(PREFIX + '/typeahead_superevent_id')
@cache.cached(query_string=True)
def typeahead_superevent_id():
    """Search GraceDB for superevents by ID.

    This involves some date parsing because GraceDB does not support directly
    searching for superevents by ID substring.
    """
    max_results = 8  # maximum number of results to return
    batch_results = 32  # batch size for results from server

    term = request.args.get('superevent_id')
    match = _typeahead_superevent_id_regex.fullmatch(term) if term else None

    if match:
        # Determine GraceDB event category from regular expression.
        prefix = match['prefix'].upper() + 'S'
        category = {'T': 'test', 'M': 'MDC'}.get(
            match['prefix'].upper(), 'production')

        # Determine start date from regular expression by padding out
        # the partial date with missing digits defaulting to 000101.
        date_partial = match['date']
        date_partial_length = len(date_partial)
        try:
            date_start = datetime.datetime.strptime(
                date_partial + '000101'[date_partial_length:], '%y%m%d')
        except ValueError:  # invalid date
            return jsonify([])

        # Determine end date from regular expression by adding a very
        # loose upper bound on the number of days until the next
        # digit in the date rolls over. No need to be exact here.
        date_end = date_start + datetime.timedelta(
            days=[36600, 3660, 366, 320, 32, 11, 1.1][date_partial_length])

        # Determine GraceDB event suffix from regular expression.
        suffix = match['suffix'].lower()
    else:
        prefix = 'S'
        category = 'production'
        date_end = datetime.datetime.utcnow()
        date_start = date_end - datetime.timedelta(days=7)
        date_partial = ''
        date_partial_length = 0
        suffix = ''

    # Query GraceDB.
    query = 'category: {} t_0: {} .. {}'.format(
        category, Time(date_start).gps, Time(date_end).gps)
    response = gracedb.client.superevents.search(
        query=query, sort='superevent_id', count=batch_results)

    # Filter superevent IDs that match the search term.
    regex = re.compile(r'{}{}\d{{{}}}{}[a-z]*'.format(
        prefix, date_partial, 6 - date_partial_length, suffix))
    superevent_ids = (
        superevent['superevent_id'] for superevent
        in response if regex.fullmatch(superevent['superevent_id']))

    # Return only the first few matches.
    return jsonify(list(take_n(max_results, superevent_ids)))


@app.route(PREFIX + '/typeahead_event_id')
@cache.cached(query_string=True)
def typeahead_event_id():
    """Search GraceDB for events by ID."""
    superevent_id = request.args.get('superevent_id').strip()
    query_terms = [f'superevent: {superevent_id}']
    if superevent_id.startswith('T'):
        query_terms.append('Test')
    elif superevent_id.startswith('M'):
        query_terms.append('MDC')
    query = ' '.join(query_terms)
    try:
        results = gracedb.get_events(query)
    except HTTPError:
        results = []
    results = [dict(r, snr=superevents.get_snr(r)) for r in results
               if superevents.is_complete(r)]
    return jsonify(list(reversed(sorted(results, key=superevents.keyfunc))))


def _search_by_tag_and_filename(superevent_id, filename, extension, tag):
    try:
        records = gracedb.get_log(superevent_id)
        return [
            '{},{}'.format(record['filename'], record['file_version'])
            for record in records if tag in record['tag_names']
            and record['filename'].startswith(filename)
            and record['filename'].endswith(extension)]
    except HTTPError as e:
        # Ignore 404 errors from server
        if e.response.status_code == 404:
            return []
        else:
            raise


@app.route(PREFIX + '/typeahead_skymap_filename')
@cache.cached(query_string=True)
def typeahead_skymap_filename():
    """Search for sky maps by filename."""
    return jsonify(_search_by_tag_and_filename(
        request.args.get('superevent_id') or '',
        request.args.get('filename') or '',
        '.multiorder.fits', 'sky_loc'
    ))


@app.route(PREFIX + '/typeahead_em_bright_filename')
@cache.cached(query_string=True)
def typeahead_em_bright_filename():
    """Search em_bright files by filename."""
    return jsonify(_search_by_tag_and_filename(
        request.args.get('superevent_id') or '',
        request.args.get('filename') or '',
        '.json', 'em_bright'
    ))


@app.route(PREFIX + '/typeahead_p_astro_filename')
@cache.cached(query_string=True)
def typeahead_p_astro_filename():
    """Search p_astro files by filename."""
    return jsonify(_search_by_tag_and_filename(
        request.args.get('superevent_id') or '',
        request.args.get('filename') or '',
        '.json', 'p_astro'
    ))


@celery_app.task(shared=False, ignore_result=True)
def _construct_igwn_alert_and_send_prelim_alert(superevent_event_list,
                                                superevent_id,
                                                initiate_voevent=True):
    superevent, event = superevent_event_list
    alert = {
        'uid': superevent_id,
        'object': superevent
    }

    orchestrator.earlywarning_preliminary_alert(
        event,
        alert,
        alert_type='preliminary',
        initiate_voevent=initiate_voevent
    )


@app.route(PREFIX + '/send_preliminary_gcn', methods=['POST'])
def send_preliminary_gcn():
    """Handle submission of preliminary alert form."""
    keys = ('superevent_id', 'event_id')
    superevent_id, event_id, *_ = tuple(request.form.get(key) for key in keys)
    if superevent_id and event_id:
        try:
            event = gracedb.get_event(event_id)
        except HTTPError as e:
            flash(f'No action performed. GraceDB query for {event_id} '
                  f'returned error code {e.response.status_code}.', 'danger')
            return redirect(url_for('index'))

        (
            gracedb.upload.s(
                None, None, superevent_id,
                'User {} queued a Preliminary alert through the dashboard.'
                .format(request.remote_user or '(unknown)'),
                tags=['em_follow'])
            |
            gracedb.update_superevent.si(
                superevent_id, preferred_event=event_id, t_0=event['gpstime'])
            |
            group(
                gracedb.get_superevent.si(superevent_id),

                gracedb.get_event.si(event_id)
            )
            |
            _construct_igwn_alert_and_send_prelim_alert.s(superevent_id)
        ).delay()
        flash('Queued preliminary alert for {}.'.format(superevent_id),
              'success')
    else:
        flash('No alert sent. Please fill in all fields.', 'danger')
    return redirect(url_for('index'))


@app.route(PREFIX + '/change_preferred_event', methods=['POST'])
def change_preferred_event():
    """Handle submission of preliminary alert form."""
    keys = ('superevent_id', 'event_id')
    superevent_id, event_id, *_ = tuple(request.form.get(key) for key in keys)
    if superevent_id and event_id:
        try:
            event = gracedb.get_event(event_id)
        except HTTPError as e:
            flash(f'No change performed. GraceDB query for {event_id} '
                  f'returned error code {e.response.status_code}.', 'danger')
            return redirect(url_for('index'))

        try:
            superevent = gracedb.get_superevent(superevent_id)
        except HTTPError as e:
            flash(f'No change performed. GraceDB query for {superevent_id} '
                  f'returned error code {e.response.status_code}.', 'danger')
            return redirect(url_for('index'))
        (
            gracedb.upload.s(
                None, None, superevent_id,
                celery_app.conf['views_manual_preferred_event_log_message']
                .format(request.remote_user or '(unknown)', event_id),
                tags=['em_follow'])
            |
            gracedb.update_superevent.si(
                superevent_id, preferred_event=event_id,
                t_0=event['gpstime'])
            |
            _construct_igwn_alert_and_send_prelim_alert.si(
                [superevent, event],
                superevent_id,
                initiate_voevent=False
            )
        ).delay()

        # Update pipeline-preferred event if the new preferred event is not
        # already the pipeline-preferred event for the pipeline that uploaded
        # it.
        pipeline_pref_event = \
            superevent['pipeline_preferred_events'].get(event['pipeline'], {})
        if pipeline_pref_event.get('graceid', '') != event_id:
            (
                gracedb.upload.s(
                    None, None, superevent_id,
                    'Manual update of preferred event triggered update of '
                    f'{event["pipeline"]}-preferred event to {event_id}',
                    tags=['em_follow'])
                |
                gracedb.add_pipeline_preferred_event.si(
                    superevent_id, event_id)
            ).delay()

        flash('Changed preferred event for {}.'.format(superevent_id),
              'success')
    else:
        flash('No change performed. Please fill in all fields.', 'danger')
    return redirect(url_for('index'))


@app.route(PREFIX + '/change_pipeline_preferred_event', methods=['POST'])
def change_pipeline_preferred_event():
    """Handle submission of preliminary alert form."""
    keys = ('superevent_id', 'pipeline', 'event_id')
    superevent_id, pipeline, event_id, *_ = tuple(request.form.get(key) for
                                                  key in keys)
    if superevent_id and pipeline and event_id:
        try:
            event = gracedb.get_event(event_id)
        except HTTPError as e:
            flash(f'No change performed. GraceDB query for {event_id} '
                  f'returned error code {e.response.status_code}.', 'danger')
            return redirect(url_for('index'))

        # Check that specified event is from specified pipeline
        if event['pipeline'].lower() != pipeline.lower():
            flash(f'No change performed. {event_id} was uploaded by '
                  f'{event["pipeline"].lower()} and cannot be the '
                  f'{pipeline.lower()}-preferred event.', 'danger')
            return redirect(url_for('index'))

        try:
            superevent = gracedb.get_superevent(superevent_id)
        except HTTPError as e:
            flash(f'No change performed. GraceDB query for {superevent_id} '
                  f'returned error code {e.response.status_code}.', 'danger')
            return redirect(url_for('index'))

        # Check that this pipeline's preferred event is not the
        # superevent's preferred event
        if superevent['preferred_event_data']['pipeline'].lower() == \
                pipeline.lower():
            flash(f'No change performed. User specified pipeline, '
                  f'{pipeline.lower()}, is the same pipeline that '
                  f'produced {superevent_id}\'s preferred event.', 'danger')
            return redirect(url_for('index'))

        (
            gracedb.upload.s(
                None, None, superevent_id,
                'User {} queued a {} preferred event change to {}.'
                .format(request.remote_user or '(unknown)', pipeline,
                        event_id),
                tags=['em_follow'])
            |
            gracedb.add_pipeline_preferred_event.si(
                superevent_id, event_id)
            |
            _construct_igwn_alert_and_send_prelim_alert.si(
                [superevent, event],
                superevent_id,
                initiate_voevent=False
            )
        ).delay()
        flash(f'Changed {pipeline.lower()} preferred event for '
              f'{superevent_id}.', 'success')
    else:
        flash('No change performed. Please fill in all fields.', 'danger')
    return redirect(url_for('index'))


@app.route(PREFIX + '/send_update_gcn', methods=['POST'])
def send_update_gcn():
    """Handle submission of update alert form."""
    keys = ('superevent_id', 'skymap_filename',
            'em_bright_filename', 'p_astro_filename')
    superevent_id, *filenames = args = tuple(
        request.form.get(key) for key in keys)
    if all(args):
        (
            gracedb.upload.s(
                None, None, superevent_id,
                'User {} queued an Update alert through the dashboard.'
                .format(request.remote_user or '(unknown)'),
                tags=['em_follow'])
            |
            orchestrator.update_alert.si(filenames, superevent_id)
        ).delay()
        flash('Queued update alert for {}.'.format(superevent_id), 'success')
    else:
        flash('No alert sent. Please fill in all fields.', 'danger')
    return redirect(url_for('index'))


@app.route(PREFIX + '/create_medium_latency_gcn_circular', methods=['POST'])
def create_medium_latency_gcn_circular():
    """Handle submission of medium_latency GCN Circular form."""
    ext_event_id = request.form.get('ext_event_id')
    if ext_event_id:
        response = make_response(circulars.create_medium_latency_circular(
            ext_event_id))
        response.headers["content-type"] = "text/plain"
        return response
    else:
        flash('No circular created. Please fill in external event ID',
              'danger')
    return redirect(url_for('index'))


@app.route(PREFIX + '/create_update_gcn_circular', methods=['POST'])
def create_update_gcn_circular():
    """Handle submission of GCN Circular form."""
    keys = ['sky_localization', 'em_bright', 'p_astro', 'raven']
    superevent_id = request.form.get('superevent_id')
    updates = [key for key in keys if request.form.get(key)]
    if superevent_id and updates:
        response = make_response(circulars.create_update_circular(
            superevent_id,
            update_types=updates))
        response.headers["content-type"] = "text/plain"
        return response
    else:
        flash('No circular created. Please fill in superevent ID and at ' +
              'least one update type.', 'danger')
    return redirect(url_for('index'))


@app.route(PREFIX + '/download_upload_external_skymap', methods=['POST'])
def download_upload_external_skymap():
    """Download sky map from URL to be uploaded to external event. Passes
    a search field 'FromURL' which indicates to get_upload_external_skymap
    to use the provided URL to download the sky map.
    """
    keys = ('ext_id', 'skymap_url')
    ext_id, skymap_url, *_ = tuple(request.form.get(key) for key in keys)
    if ext_id and skymap_url:
        ext_event = {'graceid': ext_id, 'search': 'FromURL'}
        external_skymaps.get_upload_external_skymap(
            ext_event, skymap_link=skymap_url)
        flash('Downloaded sky map for {}.'.format(ext_id),
              'success')
    else:
        flash('No skymap uploaded. Please fill in all fields.', 'danger')
    return redirect(url_for('index'))


@celery_app.task(queue='exttrig',
                 shared=False)
def _update_preferred_external_event(ext_event, superevent_id):
    """Update preferred external event to given external event."""
    # FIXME: Consider consolidating with raven.update_coinc_far by using
    # a single function in superevents.py
    if ext_event['search'] in {'GRB', 'SubGRB', 'SubGRBTargeted'}:
        coinc_far_dict = gracedb.download(
            'coincidence_far.json', ext_event['graceid'])
        if not isinstance(coinc_far_dict, dict):
            coinc_far_dict = json.loads(coinc_far_dict)
        time_coinc_far = coinc_far_dict['temporal_coinc_far']
        space_coinc_far = coinc_far_dict['spatiotemporal_coinc_far']
    else:
        time_coinc_far = None
        space_coinc_far = None
    gracedb.update_superevent(superevent_id, em_type=ext_event['graceid'],
                              time_coinc_far=time_coinc_far,
                              space_coinc_far=space_coinc_far)


@app.route(PREFIX + '/apply_raven_labels', methods=['POST'])
def apply_raven_labels():
    """Applying RAVEN alert label and update the preferred external event
    to the given coincidence."""
    keys = ('superevent_id', 'ext_id', 'event_id')
    superevent_id, ext_id, event_id, *_ = tuple(request.form.get(key)
                                                for key in keys)
    if superevent_id and ext_id and event_id:
        (
                gracedb.get_event.si(ext_id)
                |
                _update_preferred_external_event.s(superevent_id)
                |
                gracedb.create_label.si('RAVEN_ALERT', superevent_id)
                |
                gracedb.create_label.si('RAVEN_ALERT', ext_id)
                |
                gracedb.create_label.si('RAVEN_ALERT', event_id)
        ).delay()
        flash('Applied RAVEN alert label for {}.'.format(superevent_id),
              'success')
    else:
        flash('No alert sent. Please fill in all fields.', 'danger')
    return redirect(url_for('index'))


@app.route(PREFIX + '/send_mock_event', methods=['POST'])
def send_mock_event():
    """Handle submission of mock alert form."""
    first2years.upload_event.delay()
    flash('Queued a mock event.', 'success')
    return redirect(url_for('index'))


@gracedb.task(shared=False)
def _create_upload_external_event(gpstime):
    new_time = first2years_external._offset_time(
        gpstime, 'CBC', 'Fermi', 'GRB')

    ext_event = first2years_external.create_upload_external_event(
                    new_time, 'Fermi', 'MDC')

    return ext_event


@app.route(PREFIX + '/send_mock_joint_event', methods=['POST'])
def send_mock_joint_event():
    """Handle submission of mock alert form."""
    (
        first2years.upload_event.si()
        |
        _create_upload_external_event.s().set(countdown=5)
    ).delay()
    flash('Queued a mock joint event.', 'success')
    return redirect(url_for('index'))


@app.route(PREFIX + '/create_skymap_with_disabled_detectors', methods=['POST'])
def create_skymap_with_disabled_detectors():
    """Create a BAYESTAR sky map with one or more disabled detectors."""
    form = request.form.to_dict()
    graceid = form.pop('event_id')
    disabled_detectors = sorted(form.keys())
    tags = ['sky_loc', 'public']
    if graceid and disabled_detectors:
        filename = f'bayestar.no-{"".join(disabled_detectors)}.multiorder.fits'
        (
            gracedb.download.s('coinc.xml', graceid)
            |
            bayestar.localize.s(graceid, disabled_detectors=disabled_detectors)
            |
            group(
                core.identity.s(),
                gracedb.upload.s(
                    filename, graceid,
                    'sky localization complete', tags
                )
            )
            |
            skymaps.annotate_fits_tuple.s(graceid, tags)
        ).delay()
        flash('Creating sky map for event ID ' + graceid +
              ' with these disabled detectors: ' +
              ' '.join(disabled_detectors), 'success')
    else:
        flash('No sky map created. Please fill in all fields.', 'danger')
    return redirect(url_for('index'))


@app.route(PREFIX + '/copy_sky_map_between_events', methods=['POST'])
def copy_sky_map_between_events():
    superevent_id = request.form['superevent_id']
    graceid = request.form['event_id']
    skymap_filename = request.form['skymap_filename']
    skymap_filename_no_version, _, _ = skymap_filename.rpartition(',')
    tags = ['sky_loc', 'public']
    (
        gracedb.download.s(skymap_filename, graceid)
        |
        group(
            core.identity.s(),
            gracedb.upload.s(
                skymap_filename_no_version, superevent_id,
                f'sky map copied from {graceid}', tags
            )
        )
        |
        skymaps.annotate_fits_tuple.s(superevent_id, tags)
    ).delay()
    flash(f'Copying file {skymap_filename} from {graceid} to {superevent_id}',
          'success')
    return redirect(url_for('index'))
