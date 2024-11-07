"""GWSkyNet annotation with GWSkyNet model"""
import json
import re
from functools import cache

import numpy as np

from .. import app
from ..util.tempfile import NamedTemporaryFile
from . import gracedb, igwn_alert, superevents

manual_pref_event_change_regexp = re.compile(
    app.conf['views_manual_preferred_event_log_message'].replace('.', '\\.')
    .replace('{}', '.+')
)


@cache
def GWSkyNet_model():
    # FIXME Remove import from function scope once importing GWSkyNet is not a
    # slow operation
    from GWSkyNet import GWSkyNet

    return GWSkyNet.load_GWSkyNet_model()


# FIXME: run GWSkyNet on general-purpose workers
# once https://git.ligo.org/manleong.chan/gwskynet/-/issues/6 is fixed.
@app.task(queue='openmp', shared=False)
def gwskynet_annotation(input_list, SNRs, superevent_id):
    """Perform the series of tasks necessary for GWSkyNet to

    Parameters
    ----------
    input_list : list
        The output of _download_and_keep_file_name that includes the
        downloaded the skymap and the versioned file name of the skymap.
        This list is in the form [skymap, skymap_filename].
    snr : numpy array of floats
        detector SNRs.
    superevent_id : str
        superevent uid
    skymap_filename : str
        versioned filename for skymap
    """
    # FIXME Remove import from function scope once importing GWSkyNet is not a
    # slow operation
    from GWSkyNet import GWSkyNet

    filecontents, skymap_filename = input_list
    with NamedTemporaryFile(content=filecontents) as fitsfile:
        GWSkyNet_input = GWSkyNet.prepare_data(fitsfile.name)
    # One of the inputs from BAYESTAR to GWSkyNet is the list of instruments,
    # i.e., metadata['instruments'], which is converted to a binary array with
    # three elements, i.e. GWSkyNet_input[2], for H1, L1 and V1.
    # GWSkyNet 2.4.0 uses this array to indicate detector with SNR >= 4.5
    GWSkyNet_input[2][0] = np.where(SNRs >= app.conf['gwskynet_snr_threshold'],
                                    1, 0)
    gwskynet_score = GWSkyNet.predict(GWSkyNet_model(), GWSkyNet_input)
    FAP, FNP = GWSkyNet.get_rates(gwskynet_score)
    fap = FAP[0]
    fnp = FNP[0]
    gs = gwskynet_score[0]
    gwskynet_output = {'superevent_id': superevent_id,
                       'file': skymap_filename,
                       'GWSkyNet_score': gs,
                       'GWSkyNet_FAP': fap,
                       'GWSkyNet_FNP': fnp}
    return json.dumps(gwskynet_output)


def get_cbc_event_snr(event):
    """Get detector SNRs from the LVAlert packet.

    Parameters
    ----------
    event : dict
        Event dictionary (e.g., the return value from
        :meth:`gwcelery.tasks.gracedb.get_event`, or
        ``preferred_event_data`` in igwn-alert packet.)

    Returns
    -------
    snr : numpy array of floats
        detector SNRs.

    """
    # GWSkyNet 2.4.0 uses this SNR array to modify one of the inputs, so
    # snr needs to be formatted such that index 0, 1 and 2 points to H1,
    # L1 and V1 respectively
    snr = np.zeros(3)
    attribs = event['extra_attributes']['SingleInspiral']
    for det in attribs:
        if det['ifo'] == 'H1':
            snr[0] = det['snr']
        if det['ifo'] == 'L1':
            snr[1] = det['snr']
        if det['ifo'] == 'V1':
            snr[2] = det['snr']
    return snr


@gracedb.task(shared=False)
def _download_and_return_file_name(filename, graceid):
    """Wrapper around gracedb.download that returns the file name."""
    filecontents = gracedb.download(filename, graceid)
    return [filecontents, filename]


@gracedb.task(shared=False)
def _unpack_gwskynet_annotation_and_upload(gwskynet_output, graceid):
    filename = 'gwskynet.json'
    gwskynet_output_dict = json.loads(gwskynet_output)
    message = ('GWSkyNet annotation from <a href='
               '"/api/events/{graceid}/files/'
               '{skymap_filename}">'
               '{skymap_filename}</a>.'
               ' GWSkyNet score: {cs},'
               ' GWSkyNet FAP: {GWSkyNet_FAP},'
               ' GWSkyNet FNP: {GWSkyNet_FNP}.').format(
                   graceid=graceid,
                   skymap_filename=gwskynet_output_dict['file'],
                   cs=np.round(gwskynet_output_dict['GWSkyNet_score'], 3),
                   GWSkyNet_FAP=np.round(gwskynet_output_dict['GWSkyNet_FAP'],
                                         3),
                   GWSkyNet_FNP=np.round(gwskynet_output_dict['GWSkyNet_FNP'],
                                         3)
                )
    return gracedb.upload(gwskynet_output, filename, graceid, message=message,
                          tags=['em_follow', 'public'])


def _should_annotate(preferred_event, new_label, new_log_comment, labels,
                     alert_type):
    # First check if the event passes all of GWSkyNet's annotation criteria
    SNRs = get_cbc_event_snr(preferred_event)

    if not (preferred_event['search'].lower() == 'allsky' and
            preferred_event['far'] <= app.conf['gwskynet_upper_far_threshold']
            and (SNRs >= app.conf['gwskynet_snr_threshold']).sum() >= 2 and
            np.sqrt(sum(SNRs**2)) >=
            app.conf['gwskynet_network_snr_threshold']):
        return False

    annotate = False
    # Check if the GWSkyNet should annotate in response to this IGWN-Alert
    if alert_type == 'label_added':
        if superevents.should_publish(preferred_event, significant=False) is \
                False and new_label == 'SKYMAP_READY':
            # if the superevent is with FAR higher than the preliminary alert
            # threshold, GWSkyNet will anotate the superevent directly.
            annotate = True
        elif new_label == 'GCN_PRELIM_SENT' or \
                new_label == 'LOW_SIGNIF_PRELIM_SENT':
            # if the FAR is lower than the preliminary alert threshold then
            # GWSkyNet annotates the superevent if the preliminary alert has
            # been sent.
            annotate = True
    elif 'GCN_PRELIM_SENT' not in labels and 'LOW_SIGNIF_PRELIM_SENT' not in \
            labels:
        # GWSkyNet annotations not applied until after initial prelim sent when
        # FAR passes alert threshold
        pass
    elif new_log_comment.startswith('Localization copied from '):
        # GWSkyNet will also annotate the superevent if the sky map
        # has been changed (i.e. a sky map from a new g-event has been copied)
        annotate = True
    elif manual_pref_event_change_regexp.match(new_log_comment):
        # Need to check for a different log comment if the preferred event has
        # been changed manually
        annotate = True

    return annotate


@igwn_alert.handler('superevent',
                    shared=False)
def handle_cbc_superevent(alert):
    """"Annotate the CBC preferred events of superevents using GWSkyNet
    """
    if alert['object']['preferred_event_data']['group'] != 'CBC':
        return

    if alert['alert_type'] != 'label_added' and \
            alert['alert_type'] != 'log':
        return

    superevent_id = alert['uid']
    preferred_event = alert['object']['preferred_event_data']
    new_label = alert['data'].get('name', '')
    new_log_comment = alert['data'].get('comment', '')
    labels = alert['object'].get('labels', [])
    SNRs = get_cbc_event_snr(preferred_event)

    if _should_annotate(preferred_event, new_label, new_log_comment, labels,
                        alert['alert_type']):
        (
            gracedb.get_latest_file.s(superevent_id,
                                      'bayestar.multiorder.fits')
            |
            _download_and_return_file_name.s(superevent_id)
            |
            gwskynet_annotation.s(SNRs, superevent_id)
            |
            _unpack_gwskynet_annotation_and_upload.s(superevent_id)
        ).apply_async()
